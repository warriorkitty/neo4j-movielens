require 'csv'
require 'neo4j-core'

# creating a session
session = Neo4j::Session.open(:server_db, 'http://localhost:7474',
                              basic_auth: {username: 'neo4j', password: 'your-password'})

# getting genres from a CSV file
puts 'adding genres...'
genres = []
CSV.foreach('ml-latest/movies.csv', {headers: true}) do |row|

  # some movies have more genres divided by "|"
  genres += row[2].split('|')

end

# create unique genres and add them to Neo4j
genres = genres.uniq
genres.each { |genre| session.query('CREATE ( :Genre { name: {name} })', name: genre) }
puts 'genres added...'

# parse users
puts 'adding users...'
users = []
CSV.foreach('ml-latest/ratings.csv', {headers: true}) do |row|
  users << row[0] unless users.include?(row[0])
end

users.each { |user| session.query('CREATE ( :User { userId: {user_id} })', user_id: user) }
puts 'users added...'

# parsing movies
CSV.foreach('ml-latest/movies.csv', {headers: true}) do |row|
  session.query('CREATE ( :Movie { movieId: {movie_id}, title: {title} })', movie_id: row[0], title: row[1])

  # adding relationship from Movie to Genre
  row[2].split('|').each do |genre|
    session.query('MATCH ( m:Movie { movieId: {movie_id} } ), (g:Genre { name: {genre} }) CREATE (m)-[:IS_GENRE]->(g)', movie_id: row[0], genre: genre)
  end
end

# parsing rates
puts 'adding rates...'
CSV.foreach('ml-latest/ratings.csv', {headers: true}) do |row|
  session.query('MATCH (u:User { userId: {user_id} }), (m:Movie { movieId: {movie_id} })
                 CREATE (u)-[:RATE { rating: {rating}, timestamp: {timestamp} }]->(m)',
                user_id: row[0], movie_id: row[1], rating: row[2], timestamp: row[3])
end
puts 'rates added...'

# parsing tags
puts 'connecting tags with users and movies...'
CSV.foreach('ml-latest/tags.csv', {headers: true}) do |row|
  session.query('MATCH (u:User { userId: {user_id} }), (m:Movie { movieId: {movie_id} })
                 CREATE (u)-[:TAG { name: {name}, timestamp: {timestamp} }]->(m)',
                user_id: row[0], movie_id: row[1], name: row[2], timestamp: row[3])
end
puts 'tags connected with users and movies...'