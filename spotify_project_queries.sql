DROP TABLE IF EXISTS spotify_dataset;
CREATE TABLE spotify_dataset(
    artist VARCHAR(255),
    track VARCHAR(255),
    album VARCHAR(255),
    album_type VARCHAR(50),
    danceability FLOAT,
    energy FLOAT,
    loudness FLOAT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    duration_min FLOAT,
    title VARCHAR(255),
    channel VARCHAR(255),
    views FLOAT,
    likes BIGINT,
    comments BIGINT,
    licensed BOOLEAN,
    official_video BOOLEAN,
    stream BIGINT,
    energy_liveness FLOAT,
    most_played_on VARCHAR(50)
);

SELECT * 
FROM spotify_dataset;

CREATE TABLE spotify_copied_dataset
LIKE spotify_dataset;

INSERT spotify_copied_dataset
SELECT * 
FROM spotify_dataset;


-- Checking duplicate values. 
SELECT *
FROM(SELECT *,
ROW_NUMBER() OVER(PARTITION BY artist,track,album,album_type,danceability,energy,loudness, speechiness, acousticness,
	instrumentalness, liveness, valence, duration_min, title, channel, views, likes, comments, licensed, 
    official_video, stream, energy_liveness, most_played_on) as row_num
FROM spotify_copied_dataset) AS T
WHERE row_num > 1;


SELECT *
FROM spotify_copied_dataset;

SELECT DISTINCT artist
FROM spotify_copied_dataset;

SELECT DISTINCT album 
FROM spotify_copied_dataset;

SELECT DISTINCT album_type
FROM spotify_copied_dataset;

SELECT *
FROM spotify_copied_dataset;

SELECT AVG(duration_min), MAX(duration_min), MIN(duration_min)
FROM spotify_copied_dataset;

DELETE
FROM spotify_copied_dataset
WHERE duration_min = 0;

SELECT DISTINCT channel
FROM spotify_copied_dataset;


SELECT most_played_on
FROM spotify_copied_dataset
GROUP BY most_played_on;

-- Easy Level
-- 1.Retrieve the names of all tracks that have more than 1 billion streams.
	SELECT track, `stream`
    FROM spotify_copied_dataset
    WHERE stream > 1000000000;

-- 2. List all albums along with their respective artists.
	SELECT album, artist
    FROM spotify_copied_dataset;
    
-- 3. Get the total number of comments for tracks where licensed = TRUE.
	SELECT SUM(comments) AS total_comments
    FROM spotify_copied_dataset
    WHERE licensed = 1;
    
-- 4. Find all tracks that belong to the album type single.
	SELECT track
    FROM spotify_copied_dataset
    WHERE album_type = 'single';

-- 5. Count the total number of tracks by each artist.
	SELECT artist, count(track)
    FROM spotify_copied_dataset
    GROUP BY artist;
    
-- Medium Level
-- 1. Calculate the average danceability of tracks in each album.
SELECT album,  ROUND(AVG(danceability),3) as avg_danceability
FROM spotify_copied_dataset
GROUP BY album
ORDER BY 2 DESC;

-- 2. Find the top 5 tracks with the highest energy values.

SELECT *
FROM (
SELECT DISTINCT track, AVG(energy) AS avg_energy
FROM spotify_copied_dataset
GROUP BY 1
ORDER BY 2 DESC) AS energy
LIMIT 10;

SELECT *
FROM spotify_copied_dataset;




-- 3. List all tracks along with their views and likes where official_video = TRUE.
SELECT track, 
	SUM(views) AS total_views, 
    SUM(likes) AS total_likes
FROM spotify_copied_dataset
WHERE official_video = 1
GROUP BY 1
ORDER BY 2 DESC; 

-- 4. For each album, calculate the total views of all associated tracks.
SELECT album, SUM(views) as total_views_per_album
FROM spotify_copied_dataset
GROUP BY album
ORDER BY 2 desc;

-- 5. Retrieve the track names that have been streamed on Spotify more than YouTube.
SELECT *
FROM(
SELECT track, 
	SUM(CASE WHEN most_played_on = 'Spotify' THEN stream ELSE 0 END) AS stream_on_spotify,
	SUM(CASE WHEN most_played_on = 'Youtube' THEN stream ELSE 0 END) AS stream_on_youtube
FROM spotify_copied_dataset
GROUP BY 1) AS t1
WHERE stream_on_spotify > stream_on_youtube
	 AND stream_on_youtube != 0;
     
-- Advance Queries
-- 1.Find the top 3 most-viewed tracks for each artist using window functions.
SELECT artist, track, total_views
FROM(
	SELECT *, 
		DENSE_RANK() OVER(PARTITION BY artist ORDER BY total_views DESC) AS `rank`
	FROM(
		SELECT artist, track, SUM(views) as total_views
		FROM spotify_copied_dataset
		GROUP BY artist, track) as t1) AS t2
WHERE `rank` <=3;


-- 2. Write a query to find tracks where the liveness score is above the average.
SELECT track, artist, ROUND(liveness,3) as liveness
FROM spotify_copied_dataset 
	WHERE ROUND(liveness,3) >
			(SELECT ROUND(AVG(liveness),3) AS avg_liveness
			FROM spotify_copied_dataset) ;
            

-- 3. Use a WITH clause to calculate the difference between the highest and lowest energy values for tracks in each album.

WITH energy_difference AS (
	SELECT album, max(energy) max_energy, min(energy) min_energy
    FROM spotify_copied_dataset
    GROUP BY album)
SELECT *, ROUND((max_energy - min_energy),4) as energy_diff
FROM energy_difference;
