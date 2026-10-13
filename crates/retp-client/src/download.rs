//! A dataspace into a directory, one file per object - the first long
//! operation (`energyml etp download`).
//!
//! It lists what the dataspace holds (Discovery), then asks for the objects
//! by batches (Store), and writes each to `<type>_<uuid>.<ext>`. **The files
//! already there are its journal**: each is written under a temporary name
//! and renamed once complete, so a file present is a file finished, and the
//! same download run again goes on where it stopped - even after the process
//! died. Within one run, [`download_resumable`] goes further: a lost
//! connection opens a new session and the batch is asked again, a batch that
//! keeps failing is halved, and the run stops by the step's name after
//! [`Retry::max_retries`] failures in a row.

use std::collections::VecDeque;
use std::fmt;
use std::path::{Path, PathBuf};

use retp_types::datatypes::ErrorInfo;

use crate::operations::{ResourceQuery, get_data_objects, list_resources, object_file_name};
use crate::resume::{Connector, Operation, Retry, RunError, RunEvent, StepFailure, Stepped, run};
use crate::{Blocking, ClientError};

/// What to download.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Download {
    /// The dataspace's canonical URI (`eml:///` for the default one).
    pub dataspace: String,
    /// Where the files go; created if missing.
    pub directory: PathBuf,
    /// Only these types (`resqml20.obj_TriangulatedSetRepresentation`,
    /// `resqml20.*`); every type when empty.
    pub data_object_types: Vec<String>,
    /// `xml` or `json` - one the session agreed.
    pub format: String,
    /// How many objects one `GetDataObjects` asks for, at most.
    pub batch: usize,
    /// Write again the files already there, instead of taking them as done.
    pub overwrite: bool,
}

impl Download {
    /// Every object of `dataspace`, in XML, by batches of 50, into
    /// `directory`.
    pub fn new(dataspace: impl Into<String>, directory: impl Into<PathBuf>) -> Self {
        Self {
            dataspace: dataspace.into(),
            directory: directory.into(),
            data_object_types: Vec::new(),
            format: "xml".into(),
            batch: 50,
            overwrite: false,
        }
    }
}

/// Where a download is, for whoever shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Progress {
    /// The dataspace was listed: this many objects, this many already there.
    Listed {
        /// Objects in the dataspace, of the types asked for.
        objects: usize,
        /// Their files already there, from an earlier run.
        already_there: usize,
    },
    /// One more batch answered.
    Batch {
        /// Objects answered so far in this run - written or failed.
        done: usize,
        /// Objects this run has to ask for.
        total: usize,
    },
}

/// What a download did.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DownloadReport {
    /// Objects the dataspace listed, of the types asked for.
    pub listed: usize,
    /// Files written in this run.
    pub written: usize,
    /// Files already there, taken as done.
    pub already_there: usize,
    /// Objects the store answered with an error, each with its own.
    pub failed: Vec<(String, ErrorInfo)>,
    /// URIs listed that name no object, and so no file.
    pub unnamed: Vec<String>,
    /// The error that cut the listing short, if one did: the download is
    /// then partial, and says so.
    pub listing_cut_short: Option<ErrorInfo>,
}

impl DownloadReport {
    /// Whether every object listed has its file, and the listing was whole.
    pub fn complete(&self) -> bool {
        self.failed.is_empty() && self.unnamed.is_empty() && self.listing_cut_short.is_none()
    }
}

/// Why a download stopped.
#[derive(Debug)]
pub enum DownloadError {
    /// The session failed - see [`ClientError::is_connection_lost`]; the
    /// files written so far stay, and the same download goes on from them.
    Client(ClientError),
    /// A file could not be written.
    Io {
        /// Which.
        path: PathBuf,
        /// Why.
        why: std::io::Error,
    },
}

impl fmt::Display for DownloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DownloadError::Client(why) => write!(f, "{why}"),
            DownloadError::Io { path, why } => write!(f, "{}: {why}", path.display()),
        }
    }
}

impl std::error::Error for DownloadError {}

impl From<ClientError> for DownloadError {
    fn from(why: ClientError) -> Self {
        DownloadError::Client(why)
    }
}

fn io(path: &Path) -> impl FnOnce(std::io::Error) -> StepFailure<DownloadError> + '_ {
    move |why| {
        StepFailure::Final(DownloadError::Io {
            path: path.to_path_buf(),
            why,
        })
    }
}

/// A download as the runner drives it: the listing, then one batch per step.
/// A batch leaves the queue only once its files are written, so a batch cut
/// short by a lost connection is asked again, whole, in the next session.
#[derive(Debug)]
pub struct DownloadOperation {
    job: Download,
    listed: bool,
    pending: VecDeque<(String, PathBuf)>,
    total: usize,
    done: usize,
    batch: usize,
    report: DownloadReport,
    progress: Vec<Progress>,
}

impl DownloadOperation {
    /// A download not started.
    pub fn new(job: Download) -> Self {
        let batch = job.batch.max(1);
        Self {
            job,
            listed: false,
            pending: VecDeque::new(),
            total: 0,
            done: 0,
            batch,
            report: DownloadReport::default(),
            progress: Vec::new(),
        }
    }

    /// What it did so far.
    pub fn report(&self) -> &DownloadReport {
        &self.report
    }

    /// What it did.
    pub fn into_report(self) -> DownloadReport {
        self.report
    }

    fn list<K: Blocking>(&mut self, client: &mut K) -> Result<(), StepFailure<DownloadError>> {
        std::fs::create_dir_all(&self.job.directory).map_err(io(&self.job.directory))?;
        let mut query = ResourceQuery::new(&self.job.dataspace);
        query.data_object_types = self.job.data_object_types.clone();
        let listing = list_resources(client, &query)?;
        self.report.listed = listing.resources.len();
        self.report.listing_cut_short = listing.ended_by;
        for resource in &listing.resources {
            let Some(name) = object_file_name(&resource.uri, &self.job.format) else {
                self.report.unnamed.push(resource.uri.clone());
                continue;
            };
            let path = self.job.directory.join(name);
            if path.exists() && !self.job.overwrite {
                self.report.already_there += 1;
            } else {
                self.pending.push_back((resource.uri.clone(), path));
            }
        }
        self.total = self.pending.len();
        self.listed = true;
        self.progress.push(Progress::Listed {
            objects: self.report.listed,
            already_there: self.report.already_there,
        });
        Ok(())
    }

    fn fetch<K: Blocking>(&mut self, client: &mut K) -> Result<(), StepFailure<DownloadError>> {
        let size = self.batch.min(self.pending.len());
        let uris: Vec<String> = self
            .pending
            .iter()
            .take(size)
            .map(|(uri, _)| uri.clone())
            .collect();
        let answers = get_data_objects(client, &uris, &self.job.format)?;
        for _ in 0..size {
            let (uri, path) = self.pending.pop_front().expect("taken from the queue");
            match answers.get(&uri) {
                Some(Ok(object)) => {
                    let partial = path.with_extension("part");
                    std::fs::write(&partial, &object.data).map_err(io(&partial))?;
                    std::fs::rename(&partial, &path).map_err(io(&path))?;
                    self.report.written += 1;
                }
                Some(Err(error)) => self.report.failed.push((uri, error.clone())),
                None => self.report.failed.push((
                    uri,
                    ErrorInfo {
                        message: "the server answered nothing for this URI".into(),
                        code: 0,
                    },
                )),
            }
        }
        self.done += size;
        self.progress.push(Progress::Batch {
            done: self.done,
            total: self.total,
        });
        Ok(())
    }
}

impl<K: Blocking> Operation<K> for DownloadOperation {
    type Progress = Progress;
    type Error = DownloadError;

    fn step(&mut self, client: &mut K) -> Result<Stepped, StepFailure<DownloadError>> {
        if !self.listed {
            self.list(client)?;
        } else {
            self.fetch(client)?;
        }
        Ok(if self.pending.is_empty() {
            Stepped::Finished
        } else {
            Stepped::Progressed
        })
    }

    fn current_step(&self) -> String {
        if !self.listed {
            return format!("listing {}", self.job.dataspace);
        }
        let first = self.done + 1;
        let last = self.done + self.batch.min(self.pending.len());
        format!("objects {first}-{last} of {}", self.total)
    }

    fn halve(&mut self) -> bool {
        if !self.listed || self.batch <= 1 {
            return false;
        }
        self.batch = self.batch.div_ceil(2);
        true
    }

    fn take_progress(&mut self) -> Vec<Progress> {
        std::mem::take(&mut self.progress)
    }
}

/// Download `job` over one session, `client`: no new session if it falls -
/// but the files written stay, and the same download run again goes on from
/// them.
pub fn download<K: Blocking>(
    client: &mut K,
    job: &Download,
    mut progress: impl FnMut(Progress),
) -> Result<DownloadReport, DownloadError> {
    let mut operation = DownloadOperation::new(job.clone());
    loop {
        let stepped = operation.step(client);
        for p in std::mem::take(&mut operation.progress) {
            progress(p);
        }
        match stepped {
            Ok(Stepped::Finished) => return Ok(operation.into_report()),
            Ok(Stepped::Progressed) => {}
            Err(StepFailure::Session(why)) => return Err(DownloadError::Client(why)),
            Err(StepFailure::Final(why)) => return Err(why),
        }
    }
}

/// A download that stopped before its end: why, and what it had done.
#[derive(Debug)]
pub struct Stopped {
    /// Why.
    pub why: RunError<DownloadError>,
    /// What was done - the files written are there.
    pub report: DownloadReport,
}

impl fmt::Display for Stopped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} ({} written before it stopped)",
            self.why, self.report.written
        )
    }
}

impl std::error::Error for Stopped {}

/// Download `job` to its end over the sessions `connector` opens, starting
/// with `session` if one is open and leaving the last one open (ADR-0003).
pub fn download_resumable<K: Connector>(
    connector: &mut K,
    session: &mut Option<K::Session>,
    job: &Download,
    retry: &Retry,
    on_event: impl FnMut(RunEvent<Progress>),
) -> Result<DownloadReport, Box<Stopped>> {
    let mut operation = DownloadOperation::new(job.clone());
    match run(connector, session, &mut operation, retry, on_event) {
        Ok(()) => Ok(operation.into_report()),
        Err(why) => Err(Box::new(Stopped {
            why,
            report: operation.into_report(),
        })),
    }
}
