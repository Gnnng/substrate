// Copyright 2019 Parity Technologies (UK) Ltd.
// This file is part of Substrate.

// Substrate is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Substrate is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Substrate.  If not, see <http://www.gnu.org/licenses/>.

//! Block sealing utilities

use crate::{Error, rpc};
use std::sync::Arc;
use sp_runtime::{
	traits::{Block as BlockT, Header as HeaderT},
	generic::BlockId,
};
use sc_transaction_pool::txpool;
use rpc::CreatedBlock;

use sp_consensus::{
	self, BlockImport, Environment, Proposer,
	ForkChoiceStrategy, BlockImportParams, BlockOrigin,
	ImportResult, SelectChain,
	import_queue::BoxBlockImport,
};
use sp_blockchain::HeaderBackend;
use sc_client_api::backend::Backend as ClientBackend;
use hash_db::Hasher;
use std::collections::HashMap;
use std::time::Duration;
use sp_inherents::InherentDataProviders;

/// params for sealing a new block
pub struct SealBlockParams<'a, B: BlockT, C, CB, E, P: txpool::ChainApi> {
	/// if true, empty blocks(without extrinsics) will be created.
	/// otherwise, will return Error::EmptyTransactionPool.
	pub create_empty: bool,
	/// instantly finalize this block?
	pub finalize: bool,
	/// specify the parent hash of the about-to-created block
	pub parent_hash: Option<<B as BlockT>::Hash>,
	/// sender to report errors/success to the rpc.
	pub sender: rpc::Sender<CreatedBlock<<B as BlockT>::Hash>>,
	/// transaction pool
	pub pool: Arc<txpool::Pool<P>>,
	/// client backend
	pub back_end: Arc<CB>,
	/// Environment trait object for creating a proposer
	pub env: &'a mut E,
	/// SelectChain object
	pub select_chain: &'a C,
	/// block import object
	pub block_import: &'a mut BoxBlockImport<B>,
	/// inherent data provider
	pub inherent_data_provider: &'a InherentDataProviders,
}

/// seals a new block with the given params
pub async fn seal_new_block<B, C, CB, E, P, H>(params: SealBlockParams<'_, B, C, CB, E, P>)
	where
		B: BlockT,
		H: Hasher<Out=<B as BlockT>::Hash>,
		CB: ClientBackend<B, H>,
		E: Environment<B>,
		E::Error: std::fmt::Display,
		<E::Proposer as Proposer<B>>::Error: std::fmt::Display,
		P: txpool::ChainApi<Block=B, Hash=<B as BlockT>::Hash>,
		C: SelectChain<B>,
{
	let SealBlockParams {
		create_empty,
		finalize,
		pool,
		parent_hash,
		mut sender,
		back_end,
		select_chain,
		block_import,
		env,
		inherent_data_provider,
		..
	} = params;

	let future = async {
		if pool.status().ready == 0 && !create_empty {
			return Err(rpc::send_result(&mut sender, Err(Error::EmptyTransactionPool)));
		}

		// get the header to build this new block on.
		// use the parent_hash supplied via `EngineCommand`
		// or fetch the best_block.
		let header = match parent_hash {
			Some(hash) => {
				back_end.blockchain().header(BlockId::Hash(hash))
					.map_err(Error::from)
					.and_then(|header| {
						match header {
							Some(header) => Ok(header),
							None => Err(Error::BlockNotFound(format!("{}", hash)))
						}
					})
			}
			None => select_chain.best_chain().map_err(Error::from)
		};

		let header = header.map_err(|e| rpc::send_result(&mut sender, Err(e)))?;

		let mut proposer = env.init(&header)
			.map_err(|err| {
				rpc::send_result(&mut sender, Err(Error::ProposerError(format!("{}", err))))
			})?;

		let id = inherent_data_provider.create_inherent_data()
			.map_err(|err| {
				rpc::send_result(&mut sender, Err(err.into()))
			})?;

		let block = proposer.propose(id, Default::default(), Duration::from_secs(5))
			.await
			.map_err(|err| {
				rpc::send_result(&mut sender, Err(Error::ProposerError(format!("{}", err))))
			})?;

		if block.extrinsics().len() == 0 {
			return Err(rpc::send_result(&mut sender, Err(Error::EmptyTransactionPool)))
		}

		let (header, body) = block.deconstruct();
		let params = BlockImportParams {
			origin: BlockOrigin::Own,
			header: header.clone(),
			justification: None,
			post_digests: Vec::new(),
			body: Some(body),
			finalized: finalize,
			auxiliary: Vec::new(),
			fork_choice: ForkChoiceStrategy::LongestChain,
			allow_missing_state: false,
			import_existing: false,
		};

		match block_import.import_block(params, HashMap::new()) {
			Ok(ImportResult::Imported(aux)) => {
				rpc::send_result(&mut sender, Ok(CreatedBlock { hash: <B as BlockT>::Header::hash(&header), aux }))
			}
			Ok(other) => rpc::send_result(&mut sender, Err(other.into())),
			Err(e) => {
				log::warn!("Failed to import block: {:?}", e);
				rpc::send_result(&mut sender, Err(e.into()))
			}
		};

		Ok(())
	};

	let _ = future.await;
}
