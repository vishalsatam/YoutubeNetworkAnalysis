USE youtubedb

drop table if exists users;

CREATE TABLE users (
    userID VARCHAR(30) PRIMARY KEY,
    TotalVideos INT UNSIGNED,
    TotalViews INT UNSIGNED,
    MaxViewedVideo VARCHAR(30),
    MaxViews INT UNSIGNED,
    MinViewedVideo VARCHAR(30),
    MinViews INT UNSIGNED,
    AvgViews INT UNSIGNED,
    TotalLengthInMinutes INT UNSIGNED,
    AvgRating FLOAT,
    TotalRatings INT UNSIGNED,
    TotalComments INT UNSIGNED,
    MaxAgeInDays INT UNSIGNED,
    MinAgeInDays INT UNSIGNED,
    PopularityScore FLOAT,
    TotalInDegree INT UNSIGNED,
    friends INT UNSIGNED,
    kmeanlabel TINYINT
);

LOAD DATA LOCAL INFILE '/src/finalproj/data/userstatistics.csv' INTO TABLE users
FIELDS TERMINATED BY ',';

CREATE INDEX users_index ON users(userID);


