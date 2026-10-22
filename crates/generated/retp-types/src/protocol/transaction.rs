// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! `protocol::transaction` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.Transaction.CommitTransaction`.
///
/// ETP protocol 18, message 3, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Transaction.CommitTransaction")]
#[etp(protocol = 18, message_type = 3, sender = "customer", roles = "store,customer")]
pub struct CommitTransaction {
    /// `transactionUuid`.
    pub transaction_uuid: crate::datatypes::Uuid,
}

/// `Energistics.Etp.v12.Protocol.Transaction.CommitTransactionResponse`.
///
/// ETP protocol 18, message 5, sent by the store.
///
/// 2 of 3 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Transaction.CommitTransactionResponse")]
#[etp(protocol = 18, message_type = 5, sender = "store", roles = "store,customer")]
pub struct CommitTransactionResponse {
    /// `transactionUuid`.
    pub transaction_uuid: crate::datatypes::Uuid,
    /// `successful`.
    #[avro(default = true)]
    pub successful: bool,
    /// `failureReason`.
    #[avro(default = "")]
    pub failure_reason: String,
}

/// `Energistics.Etp.v12.Protocol.Transaction.RollbackTransaction`.
///
/// ETP protocol 18, message 4, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Transaction.RollbackTransaction")]
#[etp(protocol = 18, message_type = 4, sender = "customer", roles = "store,customer")]
pub struct RollbackTransaction {
    /// `transactionUuid`.
    pub transaction_uuid: crate::datatypes::Uuid,
}

/// `Energistics.Etp.v12.Protocol.Transaction.RollbackTransactionResponse`.
///
/// ETP protocol 18, message 6, sent by the store.
///
/// 2 of 3 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Transaction.RollbackTransactionResponse")]
#[etp(protocol = 18, message_type = 6, sender = "store", roles = "store,customer")]
pub struct RollbackTransactionResponse {
    /// `transactionUuid`.
    pub transaction_uuid: crate::datatypes::Uuid,
    /// `successful`.
    #[avro(default = true)]
    pub successful: bool,
    /// `failureReason`.
    #[avro(default = "")]
    pub failure_reason: String,
}

/// `Energistics.Etp.v12.Protocol.Transaction.StartTransaction`.
///
/// ETP protocol 18, message 1, sent by the customer.
///
/// 2 of 3 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Transaction.StartTransaction")]
#[etp(protocol = 18, message_type = 1, sender = "customer", roles = "store,customer")]
pub struct StartTransaction {
    /// `readOnly`.
    pub read_only: bool,
    /// `message`.
    #[avro(default = "")]
    pub message: String,
    /// `dataspaceUris`.
    #[avro(default = [""])]
    pub dataspace_uris: Vec<String>,
}

/// `Energistics.Etp.v12.Protocol.Transaction.StartTransactionResponse`.
///
/// ETP protocol 18, message 2, sent by the store.
///
/// 2 of 3 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Transaction.StartTransactionResponse")]
#[etp(protocol = 18, message_type = 2, sender = "store", roles = "store,customer")]
pub struct StartTransactionResponse {
    /// `transactionUuid`.
    pub transaction_uuid: crate::datatypes::Uuid,
    /// `successful`.
    #[avro(default = true)]
    pub successful: bool,
    /// `failureReason`.
    #[avro(default = "")]
    pub failure_reason: String,
}

