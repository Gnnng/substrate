// Copyright 2019-2020 Parity Technologies (UK) Ltd.
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
//! Inherents for BABE

use sp_inherents::{Error, InherentData, InherentIdentifier};
#[cfg(feature = "std")]
use sp_inherents::{InherentDataProviders, ProvideInherentData};
#[cfg(feature = "std")]
use sp_timestamp::TimestampInherentData;

#[cfg(feature = "std")]
use codec::Decode;
use sp_std::result::Result;

/// The BABE inherent identifier.
pub const INHERENT_IDENTIFIER: InherentIdentifier = *b"babeslot";

/// The type of the BABE inherent.
pub type InherentType = u64;
/// Auxiliary trait to extract BABE inherent data.
pub trait BabeInherentData {
    /// Get BABE inherent data.
    fn babe_inherent_data(&self) -> Result<InherentType, Error>;
    /// Replace BABE inherent data.
    fn babe_replace_inherent_data(&mut self, new: InherentType);
}

impl BabeInherentData for InherentData {
    fn babe_inherent_data(&self) -> Result<InherentType, Error> {
        self.get_data(&INHERENT_IDENTIFIER)
            .and_then(|r| r.ok_or_else(|| "BABE inherent data not found".into()))
    }

    fn babe_replace_inherent_data(&mut self, new: InherentType) {
        self.replace_data(INHERENT_IDENTIFIER, &new);
    }
}

/// Provides the slot duration inherent data for BABE.
#[cfg(feature = "std")]
pub struct InherentDataProvider {
    slot_duration: u64,
}

#[cfg(feature = "std")]
impl InherentDataProvider {
    /// Constructs `Self`
    pub fn new(slot_duration: u64) -> Self {
        Self { slot_duration }
    }
}

#[cfg(feature = "std")]
impl ProvideInherentData for InherentDataProvider {
    fn on_register(&self, providers: &InherentDataProviders) -> Result<(), Error> {
        if !providers.has_provider(&sp_timestamp::INHERENT_IDENTIFIER) {
            // Add the timestamp inherent data provider, as we require it.
            providers.register_provider(sp_timestamp::InherentDataProvider)
        } else {
            Ok(())
        }
    }

    fn inherent_identifier(&self) -> &'static InherentIdentifier {
        &INHERENT_IDENTIFIER
    }

    fn provide_inherent_data(&self, inherent_data: &mut InherentData) -> Result<(), Error> {
        let timestamp = inherent_data.timestamp_inherent_data()?;
        let slot_number = timestamp / self.slot_duration;
        inherent_data.put_data(INHERENT_IDENTIFIER, &slot_number)
    }

    fn error_to_string(&self, error: &[u8]) -> Option<String> {
        Error::decode(&mut &error[..]).map(|e| e.into_string()).ok()
    }
}
