#
- Pyflink
  - Not trivial installation, does not (fully?) support python 3.12, or even 3.11. lower versions must be used. Needs java with java home set up (also probably doesnt work with newer versions, worked with 11), plus some additional installations like gcc (nothing mentioned in docs).
  - has very tight dependencies on other libraries - mainly pemja, py4j, numpy,.. - makes it almost impossible to install pyflink in any 'bigger'(meaning e.g. env with installed pandas, pyspark or other libs using py4j) environment.
  - Not sure about iceberg compatibility - this needs to be further checked, TODO: prepare python example!

- Flink cluster deployment
  - some of the Flink conf is not read from the file - might be a race condition, need to be set in env vars directly in the docker services (jobmanager.rpc.address, otherwise the job manager and task manager are not linked together)
  - quite complicated docker image creation, there are a lot of conflicts between used libraries (aws sdk versions 1 vs 2, also latest flink 2.0 is not supported by latest io.tabular:tabular so 1.20 was used)

- Java Flink app
  - again, quite a lot of conflicts with the Flink cluster env, solved by migration to newer versions (with Flink 1.16 unresolved issues with some metric library)
  - strange config passing for aws credentials (needs to be env vars, not in parameters of the CatalogLoader)
  - note! if the data are to be loaded using Trino (from existing iceberg catalog), the CatalogLoader needs to have set the name to the catalog-name set in trino iceberg properties (carefull, not the catalog file name)

- Flink UI
  - issues with uploading identically named jars even if the previous was deleted (UI unresponsive)
  - it is not possible to deploy two identical jars (or at least identically named) with different cmd parameters? strange
  - sometimes bad error msg (the job is not submitted but the error msg is not being displayed in the UI; however visible in the job manager service)
  - not the best overview of the job state, state history, logs

- funny thing: flink has as a dependency apache beam, which is actually library, that _uses_ flink as compute engine - that's like if numpy had as a dependency pandas, or scikit; this brings additional peculiarities and troubles

- totally different apis for batch and streaming data processing (spark, in contrary, has the same api for both batch and structured streaming processing)

## summary

unless there is a need/usecase for apache flink (e.g. near realtime processing of kafka, etc.) isn't worth the trouble