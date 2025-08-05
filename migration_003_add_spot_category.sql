-- Add spot_category field to spots table
ALTER TABLE spots ADD COLUMN spot_category TEXT;

-- Create index for efficient category-based queries  
CREATE INDEX idx_spots_category ON spots(spot_category) WHERE spot_category IS NOT NULL;
