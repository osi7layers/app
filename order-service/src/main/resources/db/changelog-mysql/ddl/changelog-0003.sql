ALTER TABLE `orders`
    ADD COLUMN user_id VARCHAR(64) NULL;

CREATE INDEX idx_orders_user_id ON `orders` (user_id);
