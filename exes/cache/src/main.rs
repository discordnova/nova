use std::{error::Error, future::Future, pin::Pin};

use async_nats::{Client, Subscriber};
use managers::{
    automoderation::Automoderation, bans::Bans, channels::Channels,
    guild_schedules::GuildSchedules, guilds::Guilds, integrations::Integrations, invites::Invites,
    members::Members, messages::Messages, reactions::Reactions, roles::Roles,
    stage_instances::StageInstances, threads::Threads, CacheManager,
};
use shared::{config::Settings, payloads::CachePayload};
use tokio_stream::StreamExt;
use tracing::info;
use twilight_model::gateway::event::DispatchEvent;

use crate::config::CacheConfiguration;

mod config;
mod managers;

pub enum CacheSourcedEvents {
    None,
}

#[derive(Default)]
struct Cache {
    automoderation: Automoderation,
    channels: Channels,
    bans: Bans,
    guild_schedules: GuildSchedules,
    guilds: Guilds,
    integrations: Integrations,
    invites: Invites,
    members: Members,
    messages: Messages,
    reactions: Reactions,
    roles: Roles,
    stage_instances: StageInstances,
    threads: Threads,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let settings: Settings<CacheConfiguration> = Settings::new("cache").unwrap();
    info!("loaded configuration: {:?}", settings);
    let nats =
        Into::<Pin<Box<dyn Future<Output = anyhow::Result<Client>> + Send>>>::into(settings.nats)
            .await?;

    let mut cache = Cache::default();

    let mut sub = nats.subscribe("nova.cache.dispatch.*".to_string()).await?;
    listen(&mut sub, &mut cache, settings.config.toggles).await;
    Ok(())
}

async fn listen(sub: &mut Subscriber, cache: &mut Cache, features: Vec<String>) {
    while let Some(data) = sub.next().await {
        let cp: CachePayload = serde_json::from_slice(&data.payload).unwrap();
        let event = cp.data.0;
        match event {
            // Channel events
            DispatchEvent::ChannelCreate(_)
            | DispatchEvent::ChannelDelete(_)
            | DispatchEvent::ChannelPinsUpdate(_)
            | DispatchEvent::ChannelUpdate(_)
                if features.contains(&"channels_cache".to_string()) =>
            {
                cache.channels.handle(event).await;
            }

            // Guild Cache
            DispatchEvent::GuildCreate(_)
            | DispatchEvent::GuildDelete(_)
            | DispatchEvent::UnavailableGuild(_)
            | DispatchEvent::GuildUpdate(_)
            | DispatchEvent::WebhooksUpdate(_)
            | DispatchEvent::GuildStickersUpdate(_)
            | DispatchEvent::GuildEmojisUpdate(_)
            | DispatchEvent::VoiceServerUpdate(_)
            | DispatchEvent::GuildIntegrationsUpdate(_)
            | DispatchEvent::CommandPermissionsUpdate(_)
                if features.contains(&"guilds_cache".to_string()) =>
            {
                cache.guilds.handle(event).await;
            }

            // Guild Scheduled event
            DispatchEvent::GuildScheduledEventCreate(_)
            | DispatchEvent::GuildScheduledEventDelete(_)
            | DispatchEvent::GuildScheduledEventUpdate(_)
            | DispatchEvent::GuildScheduledEventUserAdd(_)
            | DispatchEvent::GuildScheduledEventUserRemove(_)
                if features.contains(&"guild_schedules_cache".to_string()) =>
            {
                cache.guild_schedules.handle(event).await;
            }

            // Stage events
            DispatchEvent::StageInstanceCreate(_)
            | DispatchEvent::StageInstanceDelete(_)
            | DispatchEvent::StageInstanceUpdate(_)
                if features.contains(&"stage_instances_cache".to_string()) =>
            {
                cache.stage_instances.handle(event).await;
            }

            // Integration events
            DispatchEvent::IntegrationCreate(_)
            | DispatchEvent::IntegrationDelete(_)
            | DispatchEvent::IntegrationUpdate(_)
            | DispatchEvent::InteractionCreate(_)
                if features.contains(&"integrations_cache".to_string()) =>
            {
                cache.integrations.handle(event).await;
            }

            // Member events
            DispatchEvent::MemberAdd(_)
            | DispatchEvent::MemberRemove(_)
            | DispatchEvent::MemberUpdate(_)
            | DispatchEvent::MemberChunk(_)
            | DispatchEvent::UserUpdate(_)
                if features.contains(&"members_cache".to_string()) =>
            {
                cache.members.handle(event).await;
            }

            // Ban cache
            DispatchEvent::BanAdd(_) | DispatchEvent::BanRemove(_)
                if features.contains(&"bans_cache".to_string()) =>
            {
                cache.bans.handle(event).await;
            }

            // Reaction cache
            DispatchEvent::ReactionAdd(_)
            | DispatchEvent::ReactionRemove(_)
            | DispatchEvent::ReactionRemoveAll(_)
            | DispatchEvent::ReactionRemoveEmoji(_)
                if features.contains(&"reactions_cache".to_string()) =>
            {
                cache.reactions.handle(event).await;
            }

            // Message cache
            DispatchEvent::MessageCreate(_)
            | DispatchEvent::MessageDelete(_)
            | DispatchEvent::MessageDeleteBulk(_)
            | DispatchEvent::MessageUpdate(_)
                if features.contains(&"messages_cache".to_string()) =>
            {
                cache.messages.handle(event).await;
            }

            // Thread cache
            DispatchEvent::ThreadCreate(_)
            | DispatchEvent::ThreadDelete(_)
            | DispatchEvent::ThreadListSync(_)
            | DispatchEvent::ThreadMemberUpdate(_)
            | DispatchEvent::ThreadMembersUpdate(_)
            | DispatchEvent::ThreadUpdate(_)
                if features.contains(&"threads_cache".to_string()) =>
            {
                cache.threads.handle(event).await;
            }

            // Invite cache
            DispatchEvent::InviteCreate(_) | DispatchEvent::InviteDelete(_)
                if features.contains(&"invites_cache".to_string()) =>
            {
                cache.invites.handle(event).await;
            }

            // Roles cache
            DispatchEvent::RoleCreate(_)
            | DispatchEvent::RoleDelete(_)
            | DispatchEvent::RoleUpdate(_)
                if features.contains(&"roles_cache".to_string()) =>
            {
                cache.roles.handle(event).await;
            }

            // Automod rules
            DispatchEvent::AutoModerationRuleCreate(_)
            | DispatchEvent::AutoModerationRuleDelete(_)
            | DispatchEvent::AutoModerationRuleUpdate(_)
                if features.contains(&"automoderation_cache".to_string()) =>
            {
                cache.automoderation.handle(event).await;
            }

            // Voice State
            DispatchEvent::VoiceStateUpdate(_)
                if features.contains(&"voice_states_cache".to_string()) => {}

            _ => {
                // just forward
            }
        }
    }
}
