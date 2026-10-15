//! The shell's input: stdin lines read on a thread, or, with the Cargo feature `editor` on a
//! terminal, a line editor (history, cursor keys) that lets what arrives while the user types
//! print above the prompt.
//!
//! The same API either way. A pipe or a file never uses the editor.

use std::io::BufRead;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::Receiver;

use retp_client::live::{Console, Destination};

/// The shell's input.
pub struct Input {
    inner: Inner,
}

enum Inner {
    /// Lines read from stdin on a thread of their own.
    Lines {
        lines: Receiver<std::io::Result<String>>,
        interactive: bool,
        console: Arc<dyn Destination>,
    },
    /// A line editor.
    #[cfg(feature = "editor")]
    Editor(Box<Editing>),
}

impl Input {
    /// Interactive (a terminal) or not; `history` is where the editor keeps the history.
    /// Without the `editor` feature, or when not interactive, `history` is not used.
    pub fn new(interactive: bool, history: Option<PathBuf>) -> Input {
        #[cfg(feature = "editor")]
        if interactive {
            match Editing::new(history.clone()) {
                Ok(editing) => {
                    return Input {
                        inner: Inner::Editor(Box::new(editing)),
                    };
                }
                Err(why) => eprintln!("no line editor ({why}); reading plain lines"),
            }
        }
        #[cfg(not(feature = "editor"))]
        let _ = history;
        let (typed, lines) = std::sync::mpsc::channel::<std::io::Result<String>>();
        std::thread::spawn(move || {
            for line in std::io::stdin().lock().lines() {
                if typed.send(line).is_err() {
                    break;
                }
            }
        });
        Input {
            inner: Inner::Lines {
                lines,
                interactive,
                console: Arc::new(Console::stdout()),
            },
        }
    }

    /// The next line, `None` at the end of the input. Prints `prompt` first when interactive.
    pub fn read_line(&mut self, prompt: &str) -> Option<std::io::Result<String>> {
        match &mut self.inner {
            Inner::Lines {
                lines, interactive, ..
            } => {
                if *interactive {
                    use std::io::Write;
                    print!("{prompt}");
                    std::io::stdout().flush().ok();
                }
                lines.recv().ok()
            }
            #[cfg(feature = "editor")]
            Inner::Editor(editing) => editing.read_line(prompt),
        }
    }

    /// Where to print what arrives while the user types: above the prompt with the editor,
    /// plainly on stdout without it.
    pub fn console(&self) -> Arc<dyn Destination> {
        match &self.inner {
            Inner::Lines { console, .. } => Arc::clone(console),
            #[cfg(feature = "editor")]
            Inner::Editor(editing) => editing.printed.clone(),
        }
    }
}

/// The line editor, its history file, and the printer of arrivals.
#[cfg(feature = "editor")]
struct Editing {
    editor: rustyline::DefaultEditor,
    history: Option<PathBuf>,
    printed: Arc<Printed>,
}

#[cfg(feature = "editor")]
impl Editing {
    fn new(history: Option<PathBuf>) -> Result<Editing, String> {
        let mut editor = rustyline::DefaultEditor::new().map_err(|e| e.to_string())?;
        if let Some(path) = &history {
            // No history yet is the normal first run.
            let _ = editor.load_history(path);
        }
        let printer = editor.create_external_printer().map_err(|e| e.to_string())?;
        Ok(Editing {
            editor,
            history,
            printed: Arc::new(Printed {
                printer: std::sync::Mutex::new(Box::new(printer)),
            }),
        })
    }

    fn read_line(&mut self, prompt: &str) -> Option<std::io::Result<String>> {
        use rustyline::error::ReadlineError;
        match self.editor.readline(prompt) {
            Ok(line) => {
                if !line.trim().is_empty() {
                    let _ = self.editor.add_history_entry(line.as_str());
                    if let Some(path) = &self.history {
                        let _ = self.editor.save_history(path);
                    }
                }
                Some(Ok(line))
            }
            // Ctrl-C abandons the line being typed, as in any shell; Ctrl-D ends the input.
            Err(ReadlineError::Interrupted) => Some(Ok(String::new())),
            Err(ReadlineError::Eof) => None,
            Err(ReadlineError::Io(error)) => Some(Err(error)),
            Err(other) => Some(Err(std::io::Error::other(other.to_string()))),
        }
    }
}

/// A destination that prints each arrival's line above the prompt, through the editor's
/// external printer.
#[cfg(feature = "editor")]
struct Printed {
    printer: std::sync::Mutex<Box<dyn rustyline::ExternalPrinter + Send>>,
}

#[cfg(feature = "editor")]
impl Destination for Printed {
    fn deliver(&self, event: &retp_proto::connection::Event) -> Result<(), String> {
        let line = retp_client::live::destination::as_line(event);
        let mut printer = self.printer.lock().unwrap_or_else(|p| p.into_inner());
        printer
            .print(line)
            .map_err(|e| format!("cannot print above the prompt: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plain_input_has_a_console() {
        // Non-interactive: never an editor, whatever the features.
        let input = Input::new(false, None);
        let _console: Arc<dyn Destination> = input.console();
        assert!(matches!(input.inner, Inner::Lines { .. }));
    }
}
