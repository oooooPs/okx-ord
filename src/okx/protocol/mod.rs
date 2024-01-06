pub(crate) mod brc20;
pub(crate) mod context;
pub(crate) mod execute_manager;
pub(crate) mod message;
pub(crate) mod ord;
pub(crate) mod protocol_manager;
pub(crate) mod resolve_manager;

pub use self::protocol_manager::ProtocolManager;

use {
  self::{execute_manager::CallManager, message::Message, resolve_manager::MsgResolveManager},
  crate::Options,
  bitcoin::Network,
};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct BlockContext {
  pub network: Network,
  pub blockheight: u32,
  pub blocktime: u32,
}
#[derive(Debug, Clone, Copy)]
pub struct ProtocolConfig {
  first_inscription_height: u32,
  first_brc20_height: Option<u32>,
  enable_ord_receipts: bool,
  enable_index_bitmap: bool,
}

impl ProtocolConfig {
  pub(crate) fn new_with_options(options: &Options) -> Self {
    Self {
      first_inscription_height: options.first_inscription_height(),
      first_brc20_height: Some(779832),
      enable_ord_receipts: true,
      enable_index_bitmap: false,
    }
  }
}
