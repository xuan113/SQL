INSERT OVERWRITE TABLE etl.ignite_custom_filters_d
PARTITION (test_id=6216, group_id=105)
SELECT account_id, 'Used Playlist'
FROM mramm.used_playlist_6216
WHERE used_playlist = 1;

INSERT OVERWRITE TABLE etl.ignite_custom_filters_d
PARTITION (test_id=6216, group_id=107) 
SELECT account_id, 'No Playlist'
FROM mramm.used_playlist_6216
WHERE used_playlist = 0;
