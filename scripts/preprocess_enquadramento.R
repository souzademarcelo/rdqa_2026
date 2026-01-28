library(DBI)
library(RPostgres)
library(dplyr)
library(sf)

dir.create("data/preprocessed", recursive = TRUE, showWarnings = FALSE)

pg_cfg <- list(
  dbname = Sys.getenv("PG_DBNAME", "qa_v01_22_local"),
  host = Sys.getenv("PG_HOST", "localhost"),
  port = as.integer(Sys.getenv("PG_PORT", "5432")),
  user = Sys.getenv("PG_USER", "postgres"),
  password = Sys.getenv("PG_PASSWORD", "postgres")
)

con <- dbConnect(
  RPostgres::Postgres(),
  dbname = pg_cfg$dbname,
  host = pg_cfg$host,
  port = pg_cfg$port,
  user = pg_cfg$user,
  password = pg_cfg$password
)

on.exit(dbDisconnect(con), add = TRUE)

eca_estadual_sql <- paste(
  "SELECT nome, uf, dominialidade, normativo_orientador, conselho_rh, normativo, geom",
  "FROM pgquali.qltft_0_eca_rdqa",
  "WHERE dominialidade like 'estadual'",
  "ORDER BY dominialidade, uf, nome ASC;"
)

eca_uniao_sql <- paste(
  "SELECT nome, uf, dominialidade, normativo_orientador, conselho_rh, normativo, geom",
  "FROM pgquali.qltft_0_eca_rdqa",
  "WHERE dominialidade like 'federal'",
  "ORDER BY dominialidade, uf, nome ASC;"
)

enquadramentos_estadual <- st_read(con, query = eca_estadual_sql, quiet = TRUE)
enquadramentos_uniao <- st_read(con, query = eca_uniao_sql, quiet = TRUE)

enq_estadual_final <- enquadramentos_estadual %>%
  st_make_valid() %>%
  st_simplify(preserveTopology = TRUE, dTolerance = 0.00001)

enq_uniao_final <- enquadramentos_uniao %>%
  st_make_valid() %>%
  st_simplify(preserveTopology = TRUE, dTolerance = 0.00001)

saveRDS(
  list(
    enq_estadual_final = enq_estadual_final,
    enq_uniao_final = enq_uniao_final
  ),
  file = "data/preprocessed/enquadramento.rds"
)
