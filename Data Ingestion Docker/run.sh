luigi --module dataIngestionPipeline UploadProcessedInformationToS3 --local-scheduler
echo "Luigi Tasks completed"
rm -f $TEMPPATH/*
