# Tables

- `regions` are partitioned by hash(region_name)
- `categories` are partitioned by hash(category_name), or all stored in a single partition
- `users` are colocated with the region they registered in
- `items` are colocated with the seller user
- `bids` for an item are colocated with the item
- `buy_nows` by an user are colocated with the user
- `comments` to an user are colocated with that user (recipient)
- `wins` by an user are colocated with that user.

# Indices

- A list of regions ids / names should be stored in a known partition, or known by clients in advance.
- A list of categories ids / names should be stored in a known partition, or known by clients in advance.
- The REGIONS_name (region_name -> region_id) index is partitioned by hash(region_name).
- The USERS_name (user nickname -> user_id) index is partitioned by hash(nickname).
- The USERS_region (region_id -> user_id) index is colocated with the region.
- The ITEMS_region_category ((region_id, category_id) -> item_id) index is colocated with the region.
- The COMMENTS_to_user (user_id -> comment_id) index can be colocated with the recipient user. Grow-only set.
- The BIDS_item (item_id -> bid_id) index is colocated with the item. Grow-only set.
- The BIDS_user (user_id -> bid_id) index is colocated with the user. Grow-only set.
- The BUY_NOW_user (user_id -> buy_now_id) index is colocated with the user. Grow-only set.
- The ITEMS_seller (user_id -> item_id) index is colocated with the user / item. Grow-only set.
- The WINNER_user (user_id -> win_id) index is colocated with the user. Grow-only set.

# Read Transactions

## `browse_regions()`

A list of regions should be stored in a single partition, either (1) referencing region ids, or (2) region_ids and region names.

If (1), read from partition(regions).
If (2), read from partition(regions) + all partition(region_id) (potentially all partitions).

In any case, this requires at least 2 RTTs.

## `browse_categories()`

A list of categories should be stored in a single partition, either (1) referencing category ids, or (2) category_ids and category names.

If (1), read from partition(categories).
If (2), read from partition(categories) + all partition(category_id) (potentially all partitions).

In any case, this requires at least 2 RTTs.

## `search_items_in_category(category_id, page, per_page)`

Gets all non-closed items (up to `per_page`) belonging to a category.

- Read all regions, as described in `browse_regions()`.
- For each region_id, read from ITEMS_region_category index, located in partition(region_id).

As such, read from all partitions described in `browse_regions()`, plus read from every region id to read the item object (one per region).

## `search_items_in_region(category_id, region_id, page, per_page)`

Get all non-closed items (up to per_page) belonging to a category, such that its seller is in the given region.

- Read from ITEMS_region_category, stored in partition(region_id)
- From each item id in the index, check if it is valid. (also in partition(region_id)).

It's hard to implement paging / limits with grow-only sets. Clients don't know how many keys they will get.

## `view_item(item_id)`

- Read from partition(item_id) (item object, item seller nickname)

## `view_user_info(user_id)`

Steps:
- Read user object
- Read from COMMENTS_to_user index.
- For each comment id, read the user_id for the nickname (or if we store the nickname in the comment object, don't).

Partitions:
- Read from partition(user_id) (comment object and COMMENTS_to_user index).
- If we store the nickname of the sender in the comment object, we're done. Otherwise, read from partition(user_id) for every sender identifier.

## `view_bid_history(item_id)`

Steps:
- Read the item name
- Read from the BIDS_item index, and for every bid:
    - Read the bid object, then read the nickname of the bidder

Items and bids are partitioned together, but the user for a bid might be in another partition, so it potentially reads from every partition, unless we store the nickname of the user in the index.

Partitions:
- Read from partition(item_id) (item object + BIDS_item index)
- If we store the nickname of the bidder, we're done. Otherwise, read from partition(user_id) for every bidder.

## `buy_now(item_id, user_name, password)`

Steps:
- Read from USERS_name index
- Read password from user_id, check if password matches
- Read item id
- Read nickname for seller

Partitions:
- Read from partition(user_name)
- Read from partition(user_id)

## `put_bid(item_id, nickname, password)`

Steps:
- Read from USERS_name index
- Read password from user_id, check if password matches
- Read item id, which contains max_bid and number of bids
- Read nickname for seller

Partitions:
- Read from partition(nickname)
- Read from partition(item_id)

## `put_comment(item_id, to_id, nickname, password)`

Steps:
- Read from USERS_name index
- Read password from user_id, check if password matches
- Read nickname of to_id
- Read name of item_id

Partitions:
- Read from partition(nickname)
- Read from partition(item_id)

## `select_category_to_sell_item(nickname, password)`

Same as `browse_categories`, but also checks that the user is authorized

Steps:
- Read from USERS_name index
- Read password from user_id, check if password matches
- Read all categories, as described in `browse_categories()`

Partitions:
- Read from partition(nickname)
- Read from categories partition.

## `about_me(nickname, password)`

Steps:
- Read from USERS_name index
- Read password from user_id, check if password matches
- Read user object
- Read BIDS_user index
    - For each bid id, get bid amount
- Read ITEMS_seller index to get all items sold by this user
    - For each item_id, get item object (or at least item name)
- Read WINNER_user index to get all items won by this user
    - For each win, get item name, item seller
- Read BUY_NOW_user index to get all items bought by this user
    - For each buy_now, get item name and item seller
- Read COMMENTS_to_user index to get all comments directed to this user
    - For each comment id, get comment content and comment sender nickname

Partitions:
- Read from partition(user_id) (user info, indices, sold items, comment objects)
- If we store bid amount in BIDS_user, nothing. Otherwise, read from partition(bid_id) for every bid.
- Read from partition(item_id) for every item won. We could also include this info in the index.
- Read from partition(item_id) for every item bought. We could also include this info in the index.
- Read from partition(sender_user_id) for every comment received. We could also include this info in the index.

## `get_auctions_ready_for_close(region_id, category_id, max_items, max_bids)`

Steps:
- Read from ITEMS_region_category.
    - For every item id, check that nb_of_bids > max_items, up to max_items.

Partitions:
- Read from partition(region_id).

# Update Transactions

## `register_user(region_name, nickname, first_name, last_name, email, password)`

Steps:
- Read from USERS_name, check if nickname is taken. If not, continue.
- Read from REGIONS_name, get region id
- Write user object to partition(region_id)
- Update USERS_name index
- Update USERS_region index.

Partitions:
- Read / write on partition(region_name)
- Read / write on partition(nickname)

## `store_buy_now(item_id, user_id, qty)`

Steps:
- Read item quantity, abort if not enough stock.
- Write buy_now object to partition(user_id)
- Update item object, update stock amount
- Write to BUY_NOW_user index, insert buy_now id

Partitions:
- Read / write on partition(item_id) (item object)
- Read / write on partition(user_id) (buy_now object, update BUY_NOW_user index)

## `store_bid(item_id, user_id, bid, max_bid, qty)`

Steps:
- Read item object, abort if preconditions not met.
- Write bid object to partition(item_id)
- Update item object, increment nb_of_bids / max_bid_amount
- Update BIDS_item index
- Update BIDS_user index.

Partition:
- Read / write on partition(item_id) (item object, BIDS_item index)
- Write to partition(user_id) (BIDS_user index)

## `store_comment(item_id, to_id, from_id, rating, comment)`

Steps:
- Insert comment object to partition(to_id)
- Update COMMENTS_to_user index
- Update user rating

Partitions:
- Read / write on partition(to_id) (comment object, update index, user rating)

If the COMMENTS_to_user index contains the nickname of the sender, the transactions should also read from partition(from_id) to get the nickname.

## `register_item(user_id, ...)`

Steps:
- Insert item object
- Add item id to ITEMS_region_category index.
- Add item id to ITEMS_seller index.

Partitions:
- Read / write on partition(user_id)

## `close_auction(item_id)`

Steps:
- Read item object. If closed, return.
- Get the max bid for the object. If there's no max bid, return.
- Insert winner object on the partition of the winner user.
- Update item object, set closed to true.
- Update WINNER_user index.

Partitions:
- Read / write on partition(item_id) (item object)
- Write to partition(user_id), where users_id = winner_bid.bidder_id
