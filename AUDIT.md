

looking at autit output

what do we do if a file appears to be corrupt:

example 

```json
{
    "path": "/neodc/comet/data/licsar_products/153/153D_04699_131413/20170409_20170626/20170409_20170626.geo.unw.png",
    "type": "file",
    "directory": "/neodc/comet/data/licsar_products/153/153D_04699_131413/20170409_20170626",
    "name": "20170409_20170626.geo.unw.png",
    "ext": ".png",
    "location": "on_disk",
    "size": 759459,
    "last_modified": "2022-03-10T11:49:21",
    "created": "2020-11-02T21:21:02",
    "md5": "1495e505ea0ecb2ec61c3b8c216fd562",
    "last_audit": "2025-07-28T06:21:54.695653",
    "fileset": "spot-38639-licsar_products",
    "regex_date": "2017-04-09",
    "corrupted": "2025-07-28T06:21:54.695631",
    "corrupt_md5": "015ec317bd796e78716e3bf7a83fe8ff"
}
```

This is an apparently corrupt file. What are the options?

1) Accept the file is corrupt, retrive from backup or other source, replace file with restored content. Make a not in the record that this happened.
2) Accept the file is corrupt, no useable backup. note the file is corrupt and keep as is. Flag that we have accepted that we can recover this file.
3) If we do not beleave the file looks is corrupt then flag we have accepted the newer checksum as the content.
 

How do you know its corrupt?

1) does not open in viewer or reader
2) blanks
3) storage system problems

## action to reset 
 - all cases need some record of audit fixing. 
 - Curret unfixed corruption records have a "corrupted" key and a "corrupt_md5" key
 - 

case 1) would update the record and Replace the file with the recovered one:

 ```json
{
    "path": "/neodc/comet/data/licsar_products/153/153D_04699_131413/20170409_20170626/20170409_20170626.geo.unw.png",
    "type": "file",
    "directory": "/neodc/comet/data/licsar_products/153/153D_04699_131413/20170409_20170626",
    "name": "20170409_20170626.geo.unw.png",
    "ext": ".png",
    "location": "on_disk",
    "size": 759459,
    "last_modified": "2025-10-29T09:05:43.45678",   # overwrite time
    "created": "2020-11-02T21:21:02",
    "md5": "1495e505ea0ecb2ec61c3b8c216fd562",   # recovered file has same checksum
    "last_audit": "2025-07-28T06:21:54.695653",
    "fileset": "spot-38639-licsar_products",
    "regex_date": "2017-04-09",
    "corruption_records": [
        {
            "md5": "1495e505ea0ecb2ec61c3b8c216fd562",
            "last_modified": "2022-03-10T11:49:21",
            "corrupted": "2025-07-28T06:21:54.695631",
            "corrupt_md5": "015ec317bd796e78716e3bf7a83fe8ff",
            "size": 759459,
            "reset_type": "recovered",
            "reset_date": "2025-10-29T09:05:44.12345"
        }
    ]
}
```

case 2) would update the record to reflect we can not take action to recover it:

 ```json
{
    "path": "/neodc/comet/data/licsar_products/153/153D_04699_131413/20170409_20170626/20170409_20170626.geo.unw.png",
    "type": "file",
    "directory": "/neodc/comet/data/licsar_products/153/153D_04699_131413/20170409_20170626",
    "name": "20170409_20170626.geo.unw.png",
    "ext": ".png",
    "location": "on_disk",
    "size": 759459,
    "last_modified": "2022-03-10T11:49:21",
    "created": "2020-11-02T21:21:02",
    "md5": "015ec317bd796e78716e3bf7a83fe8ff",  # corrupt checksum accepted
    "last_audit": "2025-07-28T06:21:54.695653",
    "fileset": "spot-38639-licsar_products",
    "regex_date": "2017-04-09",
    "corruption_records": [
        {
            "md5": "1495e505ea0ecb2ec61c3b8c216fd562",
            "last_modified": "2022-03-10T11:49:21",
            "corrupted": "2025-07-28T06:21:54.695631",
            "corrupt_md5": "015ec317bd796e78716e3bf7a83fe8ff",
            "size": 759459,
            "reset_type": "unrecoverable",
            "reset_date": "2025-10-29T09:05:44.12345"
        }
    ]
}
```


case 3) would update the record to reflect we do not think the file is corrupt:

 ```json
{
    "path": "/neodc/comet/data/licsar_products/153/153D_04699_131413/20170409_20170626/20170409_20170626.geo.unw.png",
    "type": "file",
    "directory": "/neodc/comet/data/licsar_products/153/153D_04699_131413/20170409_20170626",
    "name": "20170409_20170626.geo.unw.png",
    "ext": ".png",
    "location": "on_disk",
    "size": 759459,
    "last_modified": "2022-03-10T11:49:21",
    "created": "2020-11-02T21:21:02",
    "md5": "015ec317bd796e78716e3bf7a83fe8ff",  # current checksum 
    "last_audit": "2025-07-28T06:21:54.695653",
    "fileset": "spot-38639-licsar_products",
    "regex_date": "2017-04-09",
    "corruption_records": [
        {
            "md5": "1495e505ea0ecb2ec61c3b8c216fd562",
            "last_modified": "2022-03-10T11:49:21",
            "corrupted": "2025-07-28T06:21:54.695631",
            "corrupt_md5": "015ec317bd796e78716e3bf7a83fe8ff",
            "size": 759459,
            "reset_type": "false positive",
            "reset_date": "2025-10-29T09:05:44.12345"
        }
    ]
}
```


General case

 ```json
{
    "path": "/neodc/comet/data/licsar_products/153/153D_04699_131413/20170409_20170626/20170409_20170626.geo.unw.png",
    "type": "file",
    "directory": "/neodc/comet/data/licsar_products/153/153D_04699_131413/20170409_20170626",
    "name": "20170409_20170626.geo.unw.png",
    "ext": ".png",
    "location": "on_disk",
    "size": 759459,
    "last_modified": "2025-10-29T09:05:55.8765",
    "created": "2020-11-02T21:21:02",
    "md5": "1495e505ea0ecb2ec61c3b8c216fd562",
    "last_audit": "2025-07-28T06:21:54.695653",
    "fileset": "spot-38639-licsar_products",
    "regex_date": "2017-04-09",
    "change_history": [
        {
            "old_record": {
                "path": "/neodc/comet/data/licsar_products/153/153D_04699_131413/20170409_20170626/20170409_20170626.geo.unw.png",
                "type": "file",
                "directory": "/neodc/comet/data/licsar_products/153/153D_04699_131413/20170409_20170626",
                "name": "20170409_20170626.geo.unw.png",
                "ext": ".png",
                "location": "on_disk",
                "size": 759459,
                "last_modified": "2022-03-10T11:49:21",
                "created": "2020-11-02T21:21:02",
                "md5": "1495e505ea0ecb2ec61c3b8c216fd562",
                "last_audit": "2025-07-28T06:21:54.695653",
                "fileset": "spot-38639-licsar_products",
                "regex_date": "2017-04-09",
                "corrupted": "2025-07-28T06:21:54.695631",
                "corrupt_md5": "015ec317bd796e78716e3bf7a83fe8ff"
            },
            "change": "reset corrupt record ready for overwrite",
            "change_time": "2025-10-29T09:05:44.12345"
        },
        {
            "old_record": {
                "path": "/neodc/comet/data/licsar_products/153/153D_04699_131413/20170409_20170626/20170409_20170626.geo.unw.png",
                "type": "file",
                "directory": "/neodc/comet/data/licsar_products/153/153D_04699_131413/20170409_20170626",
                "name": "20170409_20170626.geo.unw.png",
                "ext": ".png",
                "location": "on_disk",
                "size": 759459,
                "last_modified": "2022-03-10T11:49:21",
                "created": "2020-11-02T21:21:02",
                "md5": "1495e505ea0ecb2ec61c3b8c216fd562",
                "last_audit": "2025-07-28T06:21:54.695653",
                "fileset": "spot-38639-licsar_products",
                "regex_date": "2017-04-09"
            },
            "change": "modified",
            "change_time": "2025-10-29T09:05:55.8765"
        }
    ]
}
```