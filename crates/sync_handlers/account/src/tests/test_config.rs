use crate::config::AccountHandlerConfig;
use stonfi_distributed_sync::coordinator::TaskPriority;
use tongrid_db::entity::StateID;

#[test]
fn test_account_state_selects_its_enabled_flag_and_priority() -> anyhow::Result<()> {
    let config = AccountHandlerConfig {
        enabled: false,
        resync_enabled: true,
        fix_enabled: true,
        ..AccountHandlerConfig::default()
    };
    assert_eq!(
        config.state_settings(StateID::AccountSync)?,
        (false, TaskPriority::High)
    );
    assert_eq!(
        config.state_settings(StateID::AccountResync)?,
        (true, TaskPriority::Low)
    );
    assert_eq!(
        config.state_settings(StateID::AccountFixSync)?,
        (true, TaskPriority::Low)
    );
    assert!(config.state_settings(StateID::BlockSync).is_err());
    Ok(())
}
