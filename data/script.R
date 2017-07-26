gen = function(n) {
  a = matrix(ceiling(runif(n * n, min=0, max=100)), nrow=n)
  b = matrix(ceiling(runif(n * n, min=0, max=100)), nrow=n)
  c = a %*% b
  write.table(a, paste0(n,'a.csv'), sep="\t", col.name=F, row.names=F)
  write.table(b, paste0(n,'b.csv'), sep="\t", col.name=F, row.names=F)
  write.table(c, paste0(n,'c.csv'), sep="\t", col.name=F, row.names=F)
}

gen(10)
gen(100)
gen(1000)
