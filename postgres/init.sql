-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Set default timezone
SET timezone = 'UTC';

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS banking;

-- Set search path
SET search_path TO banking, public;

-- Set default tablespace parameters
ALTER DATABASE banking SET default_tablespace = '';
ALTER DATABASE banking SET temp_tablespaces = '';

-- Set work_mem for the session
SET work_mem = '52428kB';

-- Set maintenance_work_mem for the session
SET maintenance_work_mem = '512MB';

-- Set effective_cache_size for the session
SET effective_cache_size = '6GB';

-- Set random_page_cost for the session
SET random_page_cost = 1.1;

-- Set effective_io_concurrency for the session
SET effective_io_concurrency = 200;

-- Set default_statistics_target for the session
SET default_statistics_target = 100; 