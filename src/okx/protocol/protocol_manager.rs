use std::thread::sleep;
use std::time::Duration;

use crate::Index;
use crate::okx::datastore::brc20::Event;
use crate::okx::datastore::{ord::OrdReaderWriter, brc20::Receipt};
use crate::okx::protocol::context::Context;
use {
  super::*,
  crate::{
    index::BlockData,
    okx::{datastore::ord::operation::InscriptionOp, protocol::ord as ord_proto},
    Instant, Result,
  },
  bitcoin::Txid,
  std::collections::HashMap,
  serde_json::{Value, json},
  ureq::{Error, Response},
};

const PUSH_BACKOFF_FACTOR: Duration = Duration::from_secs(1);
const PUSH_TIMEOUT: Duration = Duration::from_secs(1800);

pub struct ProtocolManager {
  config: ProtocolConfig,
  call_man: CallManager,
  resolve_man: MsgResolveManager,
}

impl ProtocolManager {
  // Need three datastore, and they're all in the same write transaction.
  pub fn new(config: ProtocolConfig) -> Self {
    Self {
      config,
      call_man: CallManager::new(),
      resolve_man: MsgResolveManager::new(config),
    }
  }

  pub(crate) fn index_block(
    &self,
    context: &mut Context,
    block: &BlockData,
    operations: HashMap<Txid, Vec<InscriptionOp>>,
    index: &Index,
  ) -> Result {
    let start = Instant::now();
    let mut inscriptions_size = 0;
    let mut messages_size = 0;
    let mut cost1 = 0u128;
    let mut cost2 = 0u128;
    let mut cost3 = 0u128;

    let mut brc20_events: Vec<Value> = vec![];
    let mut push_count = 0 as usize;

    // skip the coinbase transaction.
    for (tx, txid) in block.txdata.iter() {
      // skip coinbase transaction.
      if tx
        .input
        .first()
        .is_some_and(|tx_in| tx_in.previous_output.is_null())
      {
        continue;
      }

      // index inscription operations.
      if let Some(tx_operations) = operations.get(txid) {
        // save all transaction operations to ord database.
        if self.config.enable_ord_receipts
          && context.chain_conf.blockheight >= self.config.first_inscription_height
        {
          let start = Instant::now();
          context.save_transaction_operations(txid, tx_operations)?;
          inscriptions_size += tx_operations.len();
          cost1 += start.elapsed().as_micros();
        }

        let start = Instant::now();
        // Resolve and execute messages.
        let messages = self
          .resolve_man
          .resolve_message(context, tx, tx_operations)?;
        cost2 += start.elapsed().as_micros();

        let start = Instant::now();
        let receipts = self.call_man.execute_message(context, txid, &messages)?;

        if let Some(_) = index.options.brc20_events_push_url() {
          if !receipts.is_empty() {
            let mut puned_receipts = vec![];
            receipts.into_iter().for_each(|receipt| match receipt.result {
              Ok(_) => puned_receipts.push(receipt),
              Err(_) => {},
            });
            if !puned_receipts.is_empty() {
              let json_receipts: Vec<Value> = puned_receipts.into_iter().map(|receipt| json!({
                "inscription_id": receipt.inscription_id.to_string(),
                "inscription_number": receipt.inscription_number,
                "op": format!("{:?}", receipt.op),
                "from": receipt.from,
                "to": receipt.to,
                "result": match receipt.result {
                  Ok(event) => match event {
                    Event::Deploy(event) => json!({
                      "tick": event.tick.to_lowercase().to_string(),
                      "supply": event.supply.to_string(),
                      "limit_per_mint": event.limit_per_mint.to_string(),
                      "decimal": event.decimal,
                      "self_mint": event.self_mint,
                    }),
                    Event::Mint(event) => json!({
                      "tick": event.tick.to_lowercase().to_string(),
                      "amount": event.amount.to_string(),
                      "msg": event.msg.unwrap_or("".to_string()),
                      "balance": json!({
                        "overall_balance": event.balance.overall_balance.to_string(),
                        "transferable_balance": event.balance.transferable_balance.to_string(),
                      }),
                      "minted": event.minted.to_string()
                    }),
                    Event::InscribeTransfer(event) => json!({
                      "tick": event.tick.to_lowercase().to_string(),
                      "amount": event.amount.to_string(),
                      "balance": json!({
                        "overall_balance": event.balance.overall_balance.to_string(),
                        "transferable_balance": event.balance.transferable_balance.to_string(),
                      }),
                      "minted": event.minted.to_string()
                    }),
                    Event::Transfer(event) => json!({
                      "tick": event.tick.to_lowercase().to_string(),
                      "amount": event.amount.to_string(),
                      "msg": event.msg.unwrap_or("".to_string()),
                      "balance": json!({
                        "overall_balance": event.balance.overall_balance.to_string(),
                        "transferable_balance": event.balance.transferable_balance.to_string(),
                      }),
                      "to_balance": json!({
                        "overall_balance": event.to_balance.overall_balance.to_string(),
                        "transferable_balance": event.to_balance.transferable_balance.to_string(),
                      }),
                      "minted": event.minted.to_string()
                    }),
                  },
                  Err(_) => json!({})
                }
              })).collect();
              push_count += json_receipts.len();
              brc20_events.push(json!({
                "txid": txid,
                "receipts": json_receipts,
                "block": context.chain_conf.blockheight
              }))
            }
          }
        }

        cost3 += start.elapsed().as_micros();
        messages_size += messages.len();
      }
    }

    if !brc20_events.is_empty() {
      if let Some(brc20_events_push_url) = index.options.brc20_events_push_url() {
        let push_start = Instant::now();
        let data = Value::Array(brc20_events);
        let mut reorg = false;
        loop {
          match index.block_hash(context.chain_conf.blockheight.checked_sub(1))? {
            Some(index_prev_blockhash) => {
              reorg = index_prev_blockhash != block.header.prev_blockhash;
            }
            _ => {}
          }
          if reorg {
              break;
          }
          match self.push_request(&brc20_events_push_url, &data) {
            Ok(_response) => {
              /* it worked */
              break;
            },
            Err(Error::Status(_code, _response)) => {
                /* the server returned an unexpected status
                  code (such as 400, 500 etc) */
                log::error!("index server response with code {_code}, retry.");
            }
            Err(_err) => {
              /* some kind of io/transport error */
              log::error!("index server response exception, err: {_err}, retry.");
            }
          }

          sleep(PUSH_BACKOFF_FACTOR);
        }
        if reorg {
          log::info!(
            "Detected reorg, do not push events to server an let ord server do its work"
          )
        } else {
          log::info!(
            "Pushed {} brc20 events to server in {} ms",
            push_count,
            (Instant::now() - push_start).as_millis(),
          );
        }

      } else {
        log::info!("collect brc20 events: {:?}", brc20_events);
      }
    }

    let bitmap_start = Instant::now();
    let mut bitmap_count = 0;
    if self.config.enable_index_bitmap {
      bitmap_count = ord_proto::bitmap::index_bitmap(context, &operations)?;
    }
    let cost4 = bitmap_start.elapsed().as_millis();

    log::info!(
      "Protocol Manager indexed block {} with ord inscriptions {}, messages {}, bitmap {} in {} ms, {}/{}/{}/{}",
      context.chain_conf.blockheight,
      inscriptions_size,
      messages_size,
      bitmap_count,
      start.elapsed().as_millis(),
      cost1/1000,
      cost2/1000,
      cost3/1000,
      cost4,
    );
    Ok(())
  }

  pub(crate) fn push_request(&self, url: &str, data: &Value) -> Result<Response, Error> {
    let response = ureq::post(url)
        .set("Content-Type", "application/json")
        .timeout(PUSH_TIMEOUT)
        .send_json(ureq::json!(&data));

    response
  }
}
