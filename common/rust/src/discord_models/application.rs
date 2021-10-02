use enumflags2::{bitflags, BitFlags};
use serde::{Deserialize, Serialize};

use super::{teams::Team, user::User};

#[bitflags]
#[repr(u64)]
#[derive(Debug, Clone, Copy)]
pub enum ApplicationFlags {
    GatewayPresence = 1 << 12,
    GatewayPresenceLimit = 1 << 13,
    GatewayGuildMembers = 1 << 14,
    GatewayGuildMembersLimited = 1 << 15,
    VerificationPendingGuildLimit = 1 << 16,
    Embedded = 1 << 17,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Application {
    pub id: String,
    pub name: String,
    pub icon: Option<String>,
    pub description: String,
    pub rpc_origins: Option<Vec<String>>,
    pub bot_public: bool,
    pub bot_require_code_grant: bool,
    pub terms_of_service_url: Option<String>,
    pub privacy_policy_url: Option<String>,
    pub owner: Option<User>,
    pub summary: String,
    pub verify_key: String,
    pub team: Option<Team>,
    pub guild_id: Option<String>,
    pub primary_sku_id: Option<String>,
    pub slug: Option<String>,
    pub cover_image: Option<String>,
    pub flags: Option<BitFlags<ApplicationFlags>>,
}
