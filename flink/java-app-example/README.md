# Iceberg Flink Demo

This demo extends the Iceberg demo setup to include Flink. Included is a standalone Flink application that generates random Lord of the Rings
records and streams them into an Iceberg table using Flink.

Build the Flink application.
```
./gradlew clean shadowJar
```

Navigate to [http://localhost:8888](http://localhost:8888) and run the `02-flink-getting-started.ipynb` to create an `lor` database.

Navigate to the Flink UI at [http://localhost:8081/#/submit](http://localhost:8081/#/submit) and upload the shadow jar located at `build/libs/flink-example-0.0.1-all.jar`.

Submit the Flink application and provide the database and output table name as parameters. (The table will be created if it does not exist).
```
--database "lor" --table "character_sightings"
```

Once the Flink application starts, data will begin streaming into the `lor.character_sightings` table. You can use the `02-flink-getting-started.ipynb` to see the results!

*output*:
```
+----------------+-------------------------+-------------------+
|character       |location                 |event_time         |
+----------------+-------------------------+-------------------+
|Grìma Wormtongue|Bridge of Khazad-dûm     |1931-08-01 09:02:00|
|Bilbo Baggins   |Ilmen                    |1693-08-01 03:06:28|
|Denethor        |Barad-dûr                |1576-01-04 17:01:59|
|Elrond          |East Road                |1738-09-04 08:07:24|
|Shadowfax       |Helm's Deep              |1977-06-10 00:28:44|
|Denethor        |Houses of Healing        |1998-02-08 12:09:05|
|Quickbeam       |Warning beacons of Gondor|1674-05-25 06:12:54|
|Faramir         |Utumno                   |1801-04-14 00:09:19|
|Legolas         |Warning beacons of Gondor|1923-02-21 10:24:55|
|Sauron          |Eithel Sirion            |1893-05-21 01:29:57|
|Gimli           |Black Gate               |1545-03-06 20:51:13|
+----------------+-------------------------+-------------------+
```

Additional optional Flink arguments:
- `--checkpoint` - Set a checkpoint interval in milliseconds (default: 10000)
- `--event_interval` -  Set a time in milliseconds to sleep between each randomly generated record (default: 5000)
