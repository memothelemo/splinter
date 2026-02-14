use std::num::NonZeroU32;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardingRange {
    /// The starting shard index (inclusive).
    from: u32,

    /// The ending shard index (inclusive).
    to: u32,

    /// The total number of shards.
    total: u32,
}

impl ShardingRange {
    /// A constant representing a single shard configuration.
    pub const ONE: Self = ShardingRange {
        from: 0,
        to: 0,
        total: 1,
    };

    /// Returns the starting shard index (inclusive).
    #[must_use]
    pub const fn from(&self) -> u32 {
        self.from
    }

    /// Returns the ending shard index (inclusive).
    #[must_use]
    pub const fn to(&self) -> u32 {
        self.to
    }

    /// Returns the total number of shards.
    #[must_use]
    pub const fn total(&self) -> u32 {
        self.total
    }
}

/// Errors that can occur when building a [`ShardingRange`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ShardingRangeError {
    /// The total number of shards must be greater than zero.
    #[error("shard total must be greater than zero")]
    TotalIsZero,

    /// The `from` index must be less than or equal to the `to` index.
    #[error("`from` must be less than or equal to `to`")]
    FromTooLarge,

    /// The `to` index must be less than the total number of shards.
    #[error("`to` must be less than `total`")]
    ToTooLarge,
}

impl ShardingRange {
    /// Builds a [`ShardingRange`] from the required arguments. Unlike the
    /// [`new_checked(...)`] function, this will panic if the requirements below
    /// are not met, and it supports constant declaration.
    ///
    /// # Errors
    ///
    /// Throws an error if:
    /// - `total` is zero,
    /// - `from` is greater than `to`,
    /// - `to` is greater than or equal to `total`.
    ///
    /// [`new_checked(...)`]: ShardingRange::new_checked
    #[must_use]
    pub const fn new(from: u32, to: u32, total: u32) -> Self {
        let Some(total) = NonZeroU32::new(total) else {
            panic!("shard total must be greater than zero");
        };

        let total = total.get();
        if from > to {
            panic!("`from` must be less than or equal to `to`");
        }

        if to >= total {
            panic!("`to` must be less than `total`");
        }

        ShardingRange { from, to, total }
    }

    /// Builds a [`ShardingRange`] from the required arguments without letting
    /// the function panic if the requirements are not met below.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `total` is zero,
    /// - `from` is greater than `to`,
    /// - `to` is greater than or equal to `total`.
    pub fn new_checked(from: u32, to: u32, total: u32) -> Result<Self, ShardingRangeError> {
        let total = NonZeroU32::new(total);
        let total = total.ok_or(ShardingRangeError::TotalIsZero)?.get();

        if from > to {
            return Err(ShardingRangeError::FromTooLarge);
        }

        if to >= total {
            return Err(ShardingRangeError::ToTooLarge);
        }

        Ok(ShardingRange { from, to, total })
    }
}
