ALTER TABLE workers ADD COLUMN accepting_work INTEGER NOT NULL DEFAULT 1 CHECK (accepting_work IN (0, 1));
