-- Sync RAG Vector Index with New Events
-- This refreshes the vector embeddings when new events are added to the star schema

ALTER INDEX ticket_master.gold.events_index SYNC;

