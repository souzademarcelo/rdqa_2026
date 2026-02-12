library(DBI)
library(dplyr)
library(readr)
library(RPostgres)
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

pontos_sql <- paste(
  "SELECT DISTINCT",
  "qltft_1_pontos_rdqa.codigo,",
  "qltft_1_pontos_rdqa.uf,",
  "qltft_1_pontos_rdqa.latitude::character varying,",
  "qltft_1_pontos_rdqa.longitude::character varying,",
  "qltft_1_pontos_rdqa.enquadramento,",
  "qltft_1_pontos_rdqa.corpo_hidrico,",
  "qltft_1_pontos_rdqa.regime,",
  "qltft_1_pontos_urbanizados.densidade,",
  "qltft_1_pontos_urbanizados.tipo",
  "FROM pgquali.qltft_1_pontos_rdqa",
  "RIGHT JOIN pgquali.qltft_1_pontos_urbanizados",
  "ON qltft_1_pontos_rdqa.codigo = qltft_1_pontos_urbanizados.codigo",
  "ORDER BY uf, codigo;",
  sep = " "
)

pontos <- dbGetQuery(con, pontos_sql) %>%
  mutate(
    tipo = tidyr::replace_na(tipo, "rural"),
    densidade = tidyr::replace_na(densidade, "vazio"),
    enquadramento = tidyr::replace_na(enquadramento, 12),
    latitude = readr::parse_number(latitude),
    longitude = readr::parse_number(longitude)
  )

serie_ini <- as.Date("2010-01-01")
serie_final <- as.Date("2024-12-31")

params <- c("iqa", "od", "dbo", "fosfototal")

prep_param <- function(parametro, pontos_df) {
  parametro_sql <- paste0(
    "SELECT codigo, data, ",
    parametro,
    " as valor, ",
    parametro,
    "_st as status ",
    "FROM pgquali.qlttb_2_par_",
    parametro,
    " WHERE ",
    parametro,
    "_st in (1,2) ORDER BY codigo"
  )

  tbl <- dbGetQuery(con, parametro_sql) %>%
    mutate(parametro = parametro) %>%
    filter(
      data >= serie_ini,
      data <= serie_final,
      codigo != "-2"
    )

  tbl <- inner_join(tbl, pontos_df, by = "codigo")

  tbl <- tbl %>%
    mutate(
      limite = case_when(
        parametro == "iqa" & enquadramento %in% c(0, 1, 2, 3, 4, 12, 99) ~ 100,
        parametro == "od" & enquadramento %in% c(0, 1) ~ 6,
        parametro == "od" & enquadramento %in% c(2, 12, 99) ~ 5,
        parametro == "od" & enquadramento == 3 ~ 4,
        parametro == "od" & enquadramento == 4 ~ 2,
        parametro == "dbo" & enquadramento == 0 ~ 1000000,
        parametro == "dbo" & enquadramento == 1 ~ 3,
        parametro == "dbo" & enquadramento %in% c(2, 12, 99) ~ 5,
        parametro == "dbo" & enquadramento == 3 ~ 10,
        parametro == "dbo" & enquadramento == 4 ~ 10,
        parametro == "fosfototal" & enquadramento <= 2 & regime == "lotico" ~ 0.1,
        parametro == "fosfototal" & enquadramento == 3 & regime == "lotico" ~ 0.15,
        parametro == "fosfototal" & enquadramento == 4 & regime == "lentico" ~ 0.15,
        parametro == "fosfototal" & enquadramento <= 1 & regime == "lentico" ~ 0.02,
        parametro == "fosfototal" & enquadramento <= 2 & regime == "lentico" ~ 0.03,
        parametro == "fosfototal" & enquadramento <= 3 & regime == "lentico" ~ 0.05,
        parametro == "fosfototal" & enquadramento <= 4 & regime == "lotico" ~ 0.15,
        parametro == "fosfototal" & enquadramento <= 4 & regime == "lentico" ~ 0.05,
        parametro == "fosfototal" & enquadramento >= 12 & regime == "lotico" ~ 0.1,
        parametro == "fosfototal" & enquadramento >= 12 & regime == "lentico" ~ 0.03,
        TRUE ~ NA_real_
      ),
      desc = case_when(
        parametro == "od" & valor < limite ~ 1,
        parametro == "od" & valor >= limite ~ 0,
        parametro == "dbo" & valor > limite ~ 1,
        parametro == "dbo" & valor <= limite ~ 0,
        parametro == "fosfototal" & valor > limite ~ 1,
        parametro == "fosfototal" & valor <= limite ~ 0,
        parametro == "iqa" & valor > limite ~ 1,
        parametro == "iqa" & valor <= limite ~ 0,
        TRUE ~ NA_real_
      )
    )

  grouped_obs <- tbl %>%
    group_by(codigo) %>%
    summarise(
      n = n(),
      Media = round(mean(valor), 2),
      Max = round(max(valor), 2),
      Min = round(min(valor), 2),
      NC = round((sum(desc, na.rm = TRUE) / n()) * 100, 2),
      De = min(data),
      Ate = max(data),
      .groups = "drop"
    ) %>%
    inner_join(pontos_df, by = "codigo") %>%
    distinct() %>%
    na.omit()

  grouped_obs <- grouped_obs %>%
    mutate(
      De = format(De, "%d/%m/%Y"),
      Ate = format(Ate, "%d/%m/%Y"),
      Classe = case_when(
        enquadramento == 12 ~ "Não enquadrado",
        enquadramento == 0 ~ "Especial",
        enquadramento == 1 ~ "Classe 1",
        enquadramento == 2 ~ "Classe 2",
        enquadramento == 3 ~ "Classe 3",
        enquadramento == 4 ~ "Classe 4",
        enquadramento == 99 ~ "Sem informação",
        is.na(enquadramento) ~ "Sem informação"
      )
    ) %>%
    rename(
      "Código" = codigo,
      "Rio" = corpo_hidrico,
      "Média" = Media,
      "Máx" = Max,
      "Mín" = Min,
      "Até" = Ate
    )

  grouped_obs <- st_as_sf(grouped_obs, coords = c("longitude", "latitude"), crs = 4674)

  list(
    tbl = tbl,
    grouped_obs = grouped_obs
  )
}

for (param in params) {
  message("Preprocessando: ", param)
  result <- prep_param(param, pontos)
  saveRDS(result, file = file.path("data/preprocessed", paste0(param, ".rds")))
}
