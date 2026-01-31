use pinocchio::{entrypoint, AccountView, Address, ProgramResult};
use solana_program_log::log;

entrypoint!(process_instruction);

pub fn process_instruction(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    log!("Hello from my pinocchio program!");
    Ok(())
}
