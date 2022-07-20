# DROP TABLES

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

songplay_table_create = (
    """
    create table songplays (
        songplay_id serial, 
        start_time timestamp not null, 
        user_id int not null, 
        level varchar, 
        song_id varchar, 
        artist_id varchar, 
        session_id int, 
        location varchar, 
        user_agent varchar,
        primary key(songplay_id),
        constraint fk_songplays_start_time
            foreign key(start_time) 
                references time(start_time),
        constraint fk_songplays_user_id
            foreign key(user_id) 
                references users(user_id),
        constraint fk_songplays_artist_id
            foreign key(artist_id) 
                references artists(artist_id)
    );
    """
)

user_table_create = (
    """
    create table users (
        user_id int, 
        first_name varchar, 
        last_name varchar, 
        gender varchar(1), 
        level varchar,
        primary key(user_id)
    );
    """
)

song_table_create = (
    """
    create table songs (
        song_id varchar, 
        title varchar not null, 
        artist_id varchar, 
        year int, 
        duration float not null,
        primary key(song_id),
        constraint fk_songs_artist_id
            foreign key(artist_id) 
                references artists(artist_id)
    );
    """
)

artist_table_create = (
    """
    create table artists (
        artist_id varchar, 
        name varchar not null, 
        location varchar, 
        latitude float, 
        longitude float,
        primary key(artist_id)
    );
    """
)

time_table_create = (
    """
    create table time (
        start_time timestamp, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday int,
        primary key (start_time)
    );
    """
)

# INSERT RECORDS

songplay_table_insert = """
    insert into songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    values (to_timestamp(%(start_time)s), %(user_id)s, %(level)s, %(song_id)s, %(artist_id)s, %(session_id)s, %(location)s, %(user_agent)s)
    on conflict (songplay_id)
    do nothing;
    """


user_table_insert = """
    insert into users values (%(user_id)s, %(first_name)s, %(last_name)s, %(gender)s, %(level)s)
    on conflict (user_id)
    do update set level=EXCLUDED.level;
    """


song_table_insert = """
    insert into songs values (%(song_id)s, %(title)s, %(artist_id)s, %(year)s, %(duration)s)
    on conflict (song_id)
    do nothing;
    """


artist_table_insert = """
    insert into artists values (%(artist_id)s, %(name)s, %(location)s, %(latitude)s, %(longitude)s)
    on conflict (artist_id)
    do nothing;
    """


time_table_insert = """
    insert into time values (to_timestamp(%(start_time)s), %(hour)s, %(day)s, %(week)s, %(month)s, %(year)s, %(weekday)s)
    on conflict (start_time)
    do nothing;
    """
    

# FIND SONGS

song_select = (
    """
    select s.song_id, s.artist_id 
    from songs s 
    inner join artists a 
        on s.artist_id=a.artist_id
    where
        s.title=%(song_title)s and a.name=%(artist_name)s;
    """
)

# QUERY LISTS

create_table_queries = [time_table_create, artist_table_create, user_table_create, song_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
