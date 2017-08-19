USE youtubedb

drop table if exists videos;

CREATE TABLE videos (
    videoID VARCHAR(30) PRIMARY KEY,
    userID VARCHAR(30),
    videoAge DOUBLE,
    category VARCHAR(30),
    videoLength INT,
    views INT,
    rating DOUBLE,
    numberRatings INT,
    comments INT,
    PageRank DOUBLE,
    InDegree DOUBLE
);

LOAD DATA LOCAL INFILE '/src/finalproj/data/MainFile_W_PR_DEG.csv' INTO TABLE videos
FIELDS TERMINATED BY ',';

CREATE INDEX view_index ON videos(views DESC);
CREATE INDEX age_index ON videos(videoAge ASC);

