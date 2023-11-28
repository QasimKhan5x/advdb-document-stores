-- Indexes for sf1
CREATE INDEX idx_sf1_created_at ON sf1 ((data ->> 'created_at'));
CREATE INDEX idx_sf1_user_location ON sf1 ((data #>> '{user, location}'));
CREATE INDEX idx_sf1_hashtags_text ON sf1 USING GIN ((data -> 'entities' -> 'hashtags' -> 'text') jsonb_path_ops);
CREATE INDEX idx_sf1_text ON sf1 USING GIN (to_tsvector('english', data ->> 'text'));
CREATE INDEX idx_sf1_user_id_str ON sf1 ((data #>> '{user, id_str}'));

-- Indexes for sf2
CREATE INDEX idx_sf2_created_at ON sf2 ((data ->> 'created_at'));
CREATE INDEX idx_sf2_user_location ON sf2 ((data #>> '{user, location}'));
CREATE INDEX idx_sf2_hashtags_text ON sf2 USING GIN ((data -> 'entities' -> 'hashtags' -> 'text') jsonb_path_ops);
CREATE INDEX idx_sf2_text ON sf2 USING GIN (to_tsvector('english', data ->> 'text'));
CREATE INDEX idx_sf2_user_id_str ON sf2 ((data #>> '{user, id_str}'));

-- Indexes for sf3
CREATE INDEX idx_sf3_created_at ON sf3 ((data ->> 'created_at'));
CREATE INDEX idx_sf3_user_location ON sf3 ((data #>> '{user, location}'));
CREATE INDEX idx_sf3_hashtags_text ON sf3 USING GIN ((data -> 'entities' -> 'hashtags' -> 'text') jsonb_path_ops);
CREATE INDEX idx_sf3_text ON sf3 USING GIN (to_tsvector('english', data ->> 'text'));
CREATE INDEX idx_sf3_user_id_str ON sf3 ((data #>> '{user, id_str}'));

-- Indexes for sf4
CREATE INDEX idx_sf4_created_at ON sf4 ((data ->> 'created_at'));
CREATE INDEX idx_sf4_user_location ON sf4 ((data #>> '{user, location}'));
CREATE INDEX idx_sf4_hashtags_text ON sf4 USING GIN ((data -> 'entities' -> 'hashtags' -> 'text') jsonb_path_ops);
CREATE INDEX idx_sf4_text ON sf4 USING GIN (to_tsvector('english', data ->> 'text'));
CREATE INDEX idx_sf4_user_id_str ON sf4 ((data #>> '{user, id_str}'));

-- Indexes for sf5
CREATE INDEX idx_sf5_created_at ON sf5 ((data ->> 'created_at'));
CREATE INDEX idx_sf5_user_location ON sf5 ((data #>> '{user, location}'));
CREATE INDEX idx_sf5_hashtags_text ON sf5 USING GIN ((data -> 'entities' -> 'hashtags' -> 'text') jsonb_path_ops);
CREATE INDEX idx_sf5_text ON sf5 USING GIN (to_tsvector('english', data ->> 'text'));
CREATE INDEX idx_sf5_user_id_str ON sf5 ((data #>> '{user, id_str}'));