SQLite = read.csv("/Users/carolyn/CL_SQLite.csv")

points <- SQLite[,1]
insert <- SQLite[,2]
first <- SQLite[,3]
last <- SQLite[,4]
range <- SQLite[,5]

plot(points, insert, type='p')
plot(points, first, type='p')
plot(points, last, type='p')
plot(points, range, type='p')

# line of best fit
res = lm(range~points)
abline(res)

##############################################

influx = read.csv("/Users/carolyn/CL_Influx.csv")

points <- influx[,1]
insert <- influx[,2]
first <- influx[,3]
last <- influx[,4]
range <- influx[,5]

plot(points, insert, type='p')
plot(points, first, type='p')
plot(points, last, type='p')
plot(points, range, type='p')

res = lm(range~points)
abline(res)

################################################

tempo = read.csv("/Users/carolyn/CL_Tempo.csv")

points <- tempo[,1]
insert <- tempo[,2]
first <- tempo[,3]
last <- tempo[,4]
range <- tempo[,5]

plot(points, insert, type='p')
plot(points, first, type='p')
plot(points, last, type='p')
plot(points, range, type='p')

res = lm(range~points)
abline(res)

