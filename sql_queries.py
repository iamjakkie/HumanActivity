create_People = ("""
    CREATE TABLE IF NOT EXISTS People(
    Id BIGINT PRIMARY KEY,
    Name VARCHAR NOT NULL)
""")

create_Sources = ("""
    CREATE TABLE IF NOT EXISTS Sources(
    Id BIGINT PRIMARY KEY,
    Name VARCHAR NOT NULL)
""")

create_Collectibles = ("""
    CREATE TABLE IF NOT EXISTS Collectibles(
    Id BIGINT PRIMARY KEY,
    Name VARCHAR NOT NULL)
""")

create_Time = ("""
    CREATE TABLE IF NOT EXISTS Time(
    Id BIGINT PRIMARY KEY,
    Date DATE NOT NULL,
    Year BIGINT NOT NULL,
    Month BIGINT NOT NULL,
    Day BIGINT NOT NULL,
    Hour BIGINT NOT NULL,
    Minute BIGINT NOT NULL)
""")

create_Activities = ("""
    CREATE TABLE IF NOT EXISTS FactActivities(
    TimeId BIGINT NOT NULL,
    PersonId BIGINT NOT NULL,
    SourceId BIGINT NOT NULL,
    CollectibleId BIGINT NOT NULL,
    Values VARCHAR NOT NULL,
    FOREIGN KEY(TimeId) REFERENCES Time(Id),
    FOREIGN KEY(PeopleId) REFERENCES People(Id),
    FOREIGN KEY(SourceId) REFERENCES Sources(Id),
    FOREIGN KEY(CollectibleId) REFERENCES Collectibles(Id))
""")

tables = ['people', 'sources', 'collectibles', 'time', 'activities']
create_queries = [create_People, create_Sources, create_Collectibles, create_Time, create_Activities]
create_tables = dict(zip(tables,create_queries))

