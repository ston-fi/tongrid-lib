use strum_macros::{Display, EnumString, IntoStaticStr};

#[derive(Display, Debug, Clone, PartialEq, Eq, Hash, EnumString, IntoStaticStr, Copy)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum StateID {
    BlockSync,
    BlockResync,
    BackupBlockSync,

    AccountSync,
    AccountResync,
    AccountFixSync,
    BackupAccountSync,

    ClassifierSync,
    ClassifierResync,
    BackupClassifierSync,

    JettonSync,
    JettonResync,
    BackupJettonSync,

    VerifierSync,
    CustomResync,

    BackupRestoreBlock,
    BackupRestoreAccount,
    BackupRestoreClassifier,
    BackupRestoreJetton,
}

impl AsRef<str> for StateID {
    fn as_ref(&self) -> &str {
        self.into()
    }
}
