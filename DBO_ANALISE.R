
library(dplyr)
library(lubridate)
library(tidyr)
library(dygraphs)

# NC% por ano e regime, excluindo CE
dbo_desc_regime_sem_ce <- tbl_dbo %>%
  filter(
    regime %in% c("lentico", "lotico"),
    uf != "CE"
  ) %>%
  group_by(ano = year(data), regime) %>%
  summarise(
    nao_nulos = sum(!is.na(desc)),
    NC = sum(desc == 1, na.rm = TRUE),
    .groups = "drop"
  )

anos_dbo_sem_ce <- seq(
  min(year(tbl_dbo$data), na.rm = TRUE),
  max(2024, max(year(tbl_dbo$data), na.rm = TRUE))
)

dbo_desc_regime_sem_ce <- dbo_desc_regime_sem_ce %>%
  complete(
    ano = anos_dbo_sem_ce,
    regime = c("lentico", "lotico"),
    fill = list(nao_nulos = 0, NC = 0)
  ) %>%
  mutate(freq_nc = if_else(nao_nulos == 0, NA_real_, (NC / nao_nulos) * 100))

freq_regime_dbo_sem_ce <- dbo_desc_regime_sem_ce %>%
  select(ano, regime, freq_nc) %>%
  pivot_wider(names_from = regime, values_from = freq_nc) %>%
  select(ano, lentico, lotico)

dygraph(freq_regime_dbo_sem_ce) %>%
  dySeries("lentico", label = "NC(%) lêntico (sem CE)", color = "#a50f15") %>%
  dySeries("lotico", label = "NC(%) lótico (sem CE)", color = "#faae91") %>%
  dyOptions(
    drawPoints = TRUE,
    pointSize = 5,
    strokeWidth = 3,
    connectSeparatedPoints = TRUE
  ) %>%
  dyAxis("y", label = "Percentual(%)") %>%
  dyAxis("x", label = "Ano")






library(dplyr)
library(tidyr)
library(lubridate)
library(dygraphs)

# UFs para comparação
ufs_alvo <- c("CE", "RN", "PE")

anos <- seq(
  min(year(tbl_dbo$data), na.rm = TRUE),
  max(2024, max(year(tbl_dbo$data), na.rm = TRUE))
)

conf_uf <- tbl_dbo %>%
  filter(
    regime == "lentico",
    uf %in% ufs_alvo
  ) %>%
  group_by(ano = year(data), uf) %>%
  summarise(
    nao_nulos = sum(!is.na(desc)),
    NC = sum(desc == 1, na.rm = TRUE),
    conformidade = if_else(nao_nulos == 0, NA_real_, 100 - (NC / nao_nulos) * 100),
    .groups = "drop"
  ) %>%
  complete(ano = anos, uf = ufs_alvo)

df_conf <- conf_uf %>%
  select(ano, uf, conformidade) %>%
  pivot_wider(names_from = uf, values_from = conformidade) %>%
  arrange(ano)

dygraph(df_conf) %>%
  dySeries("CE", label = "Conformidade CE (%)", color = "#d62728") %>%
  dySeries("RN", label = "Conformidade RN (%)", color = "#1f77b4") %>%
  dySeries("PE", label = "Conformidade PE (%)", color = "#2ca02c") %>%
  dyOptions(
    drawPoints = TRUE,
    pointSize = 4,
    strokeWidth = 3,
    connectSeparatedPoints = TRUE
  ) %>%
  dyAxis("y", label = "Conformidade (%)") %>%
  dyAxis("x", label = "Ano")

##################################################################################



library(dplyr)
library(tidyr)
library(lubridate)
library(dygraphs)

# UFs para comparação (somente regime lentico)
ufs_alvo <- c("CE", "RN", "PE")

anos <- seq(
  min(year(tbl_dbo$data), na.rm = TRUE),
  max(2024, max(year(tbl_dbo$data), na.rm = TRUE))
)

nc_uf <- tbl_dbo %>%
  filter(
    regime == "lentico",
    uf %in% ufs_alvo
  ) %>%
  group_by(ano = year(data), uf) %>%
  summarise(
    nao_nulos = sum(!is.na(desc)),
    NC = sum(desc == 1, na.rm = TRUE),
    freq_nc = if_else(nao_nulos == 0, NA_real_, (NC / nao_nulos) * 100),
    .groups = "drop"
  ) %>%
  complete(ano = anos, uf = ufs_alvo)

df_nc <- nc_uf %>%
  select(ano, uf, freq_nc) %>%
  pivot_wider(names_from = uf, values_from = freq_nc) %>%
  arrange(ano)

dygraph(df_nc) %>%
  dySeries("CE", label = "NC CE (%)", color = "#d62728") %>%
  dySeries("RN", label = "NC RN (%)", color = "#1f77b4") %>%
  dySeries("PE", label = "NC PE (%)", color = "#2ca02c") %>%
  dyOptions(
    drawPoints = TRUE,
    pointSize = 4,
    strokeWidth = 3,
    connectSeparatedPoints = TRUE
  ) %>%
  dyAxis("y", label = "Não-Conformidade (%)") %>%
  dyAxis("x", label = "Ano")

###########################################################################

library(dplyr)
library(tidyr)
library(lubridate)
library(dygraphs)

ufs_destaque <- c("CE", "RN", "PE")

anos <- seq(
  min(year(tbl_dbo$data), na.rm = TRUE),
  max(2024, max(year(tbl_dbo$data), na.rm = TRUE))
)

nc_abs <- tbl_dbo %>%
  filter(regime == "lentico") %>%
  mutate(
    grupo_uf = if_else(uf %in% ufs_destaque, uf, "Outras UFs"),
    ano = year(data)
  ) %>%
  group_by(ano, grupo_uf) %>%
  summarise(
    NC_abs = sum(desc == 1, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  complete(ano = anos, grupo_uf = c("CE", "RN", "PE", "Outras UFs"), fill = list(NC_abs = 0)) %>%
  pivot_wider(names_from = grupo_uf, values_from = NC_abs) %>%
  arrange(ano)

dygraph(nc_abs) %>%
  dySeries("CE", label = "NC CE (abs)", color = "#d62728") %>%
  dySeries("RN", label = "NC RN (abs)", color = "#1f77b4") %>%
  dySeries("PE", label = "NC PE (abs)", color = "#2ca02c") %>%
  dySeries("Outras UFs", label = "NC Outras UFs (abs)", color = "#6b7280") %>%
  dyOptions(
    drawPoints = TRUE,
    pointSize = 4,
    strokeWidth = 3,
    connectSeparatedPoints = TRUE
  ) %>%
  dyAxis("y", label = "Número absoluto de NC") %>%
  dyAxis("x", label = "Ano")




library(dplyr)
library(tidyr)
library(lubridate)
library(purrr)
library(ggplot2)

# ---------------------------
# Base CE - DBO - lentico
# ---------------------------
df <- tbl_dbo %>%
  filter(uf == "CE", regime == "lentico") %>%
  mutate(
    data = as.Date(data),
    ano = year(data),
    mes = month(data),
    valido = !is.na(desc),
    nc_bin = if_else(desc == 1, 1L, 0L, missing = 0L)
  ) %>%
  filter(!is.na(ano), ano >= 2010, ano <= 2024)

base_ref <- df %>% filter(ano >= 2020, ano <= 2023)
ano_alvo <- df %>% filter(ano == 2024)

if (nrow(ano_alvo) == 0) stop("Nao ha dados de CE/lentico em 2024.")

cat("\n===== 1) Cobertura de amostragem =====\n")
cobertura_ano <- df %>%
  group_by(ano) %>%
  summarise(
    n_obs = n(),
    n_validos = sum(valido),
    n_codigos = n_distinct(codigo),
    nc = sum(nc_bin[valido], na.rm = TRUE),
    nc_pct = 100 * nc / pmax(n_validos, 1),
    .groups = "drop"
  )
print(cobertura_ano)

cobertura_mes_2024 <- ano_alvo %>%
  group_by(mes) %>%
  summarise(
    n_obs = n(),
    n_validos = sum(valido),
    n_codigos = n_distinct(codigo),
    nc = sum(nc_bin[valido], na.rm = TRUE),
    nc_pct = 100 * nc / pmax(n_validos, 1),
    .groups = "drop"
  )
print(cobertura_mes_2024)

print(
  ggplot(cobertura_ano, aes(ano, nc_pct)) +
    geom_line(color = "#b91c1c", linewidth = 1) +
    geom_point(color = "#b91c1c", size = 2) +
    labs(title = "CE lentico: evolucao da NC (%)", x = "Ano", y = "NC (%)") +
    theme_minimal()
)

cat("\n===== 2) Mudanca de composicao de pontos =====\n")
codigos_ref <- unique(base_ref$codigo)
comp_2024 <- ano_alvo %>%
  mutate(grupo_ponto = if_else(codigo %in% codigos_ref, "Ponto antigo", "Ponto novo"))

print(comp_2024 %>% count(grupo_ponto, name = "n_obs_2024"))
print(comp_2024 %>% group_by(grupo_ponto) %>%
        summarise(
          n_validos = sum(valido),
          nc = sum(nc_bin[valido], na.rm = TRUE),
          nc_pct = 100 * nc / pmax(n_validos, 1),
          n_codigos = n_distinct(codigo),
          .groups = "drop"
        ))

cat("\n===== 3) Consistencia de metadados (limite/enquadramento/status) =====\n")
vars_meta <- c("limite", "enquadramento", "status")
vars_meta <- vars_meta[vars_meta %in% names(df)]

if (length(vars_meta) > 0) {
  for (v in vars_meta) {
    cat("\n--- Variavel:", v, "---\n")
    tab <- df %>%
      group_by(codigo) %>%
      summarise(n_val_distintos = n_distinct(.data[[v]], na.rm = TRUE), .groups = "drop") %>%
      arrange(desc(n_val_distintos))
    print(head(tab, 20))
  }
  
  # possiveis trocas de classe/limite em 2024 vs 2020-2023
  if (all(c("limite", "enquadramento") %in% names(df))) {
    ref_mode <- base_ref %>%
      group_by(codigo) %>%
      summarise(
        limite_ref = as.numeric(names(sort(table(limite), decreasing = TRUE))[1]),
        enquad_ref = as.numeric(names(sort(table(enquadramento), decreasing = TRUE))[1]),
        .groups = "drop"
      )
    
    alvo_mode <- ano_alvo %>%
      group_by(codigo) %>%
      summarise(
        limite_2024 = as.numeric(names(sort(table(limite), decreasing = TRUE))[1]),
        enquad_2024 = as.numeric(names(sort(table(enquadramento), decreasing = TRUE))[1]),
        .groups = "drop"
      )
    
    mudancas_meta <- ref_mode %>%
      inner_join(alvo_mode, by = "codigo") %>%
      mutate(
        mudou_limite = limite_ref != limite_2024,
        mudou_enquadramento = enquad_ref != enquad_2024
      ) %>%
      filter(mudou_limite | mudou_enquadramento)
    
    print(mudancas_meta)
  }
}

cat("\n===== 4) Qualidade dos valores analiticos =====\n")
qualidade_ano <- df %>%
  group_by(ano) %>%
  summarise(
    n = n(),
    media = mean(valor, na.rm = TRUE),
    p50 = quantile(valor, 0.50, na.rm = TRUE),
    p90 = quantile(valor, 0.90, na.rm = TRUE),
    p95 = quantile(valor, 0.95, na.rm = TRUE),
    max = max(valor, na.rm = TRUE),
    .groups = "drop"
  )
print(qualidade_ano)

# outliers 2024 usando limites IQR da base 2020-2023
q1 <- quantile(base_ref$valor, 0.25, na.rm = TRUE)
q3 <- quantile(base_ref$valor, 0.75, na.rm = TRUE)
iqr_val <- q3 - q1
lim_inf <- q1 - 1.5 * iqr_val
lim_sup <- q3 + 1.5 * iqr_val

out_2024 <- ano_alvo %>%
  mutate(outlier_iqr = valor < lim_inf | valor > lim_sup) %>%
  summarise(
    n = n(),
    n_out = sum(outlier_iqr, na.rm = TRUE),
    pct_out = 100 * n_out / pmax(n, 1)
  )
print(out_2024)

# repeticao de valor exato por ponto-mes
rep_2024 <- ano_alvo %>%
  group_by(codigo, mes, valor) %>%
  summarise(n_rep = n(), .groups = "drop") %>%
  filter(n_rep >= 3) %>%
  arrange(desc(n_rep))
print(head(rep_2024, 30))

cat("\n===== 5) Influencia por ponto (leave-one-station-out) =====\n")
nc_global_2024 <- ano_alvo %>%
  summarise(
    n_validos = sum(valido),
    nc = sum(nc_bin[valido], na.rm = TRUE),
    nc_pct = 100 * nc / pmax(n_validos, 1)
  )
print(nc_global_2024)

impacto_ponto <- map_dfr(unique(ano_alvo$codigo), function(cd) {
  d <- ano_alvo %>% filter(codigo != cd)
  n_validos <- sum(d$valido)
  nc <- sum(d$nc_bin[d$valido], na.rm = TRUE)
  nc_pct_sem <- 100 * nc / pmax(n_validos, 1)
  tibble(
    codigo = cd,
    nc_pct_sem_ponto = nc_pct_sem,
    impacto_pp = nc_global_2024$nc_pct - nc_pct_sem
  )
}) %>%
  arrange(desc(impacto_pp))

print(head(impacto_ponto, 20))

cat("\n===== 6) Teste formal de mudanca de proporcao =====\n")
base_ref_stats <- base_ref %>%
  summarise(
    n_validos = sum(valido),
    nc = sum(nc_bin[valido], na.rm = TRUE),
    .groups = "drop"
  )

ano_2024_stats <- ano_alvo %>%
  summarise(
    n_validos = sum(valido),
    nc = sum(nc_bin[valido], na.rm = TRUE),
    .groups = "drop"
  )

teste_agregado <- prop.test(
  x = c(base_ref_stats$nc, ano_2024_stats$nc),
  n = c(base_ref_stats$n_validos, ano_2024_stats$n_validos),
  correct = FALSE
)
print(teste_agregado)

# teste por ponto (somente pontos com tamanho minimo nos 2 periodos)
teste_ponto <- df %>%
  filter(ano >= 2020, ano <= 2024) %>%
  mutate(periodo = if_else(ano == 2024, "y2024", "ref")) %>%
  group_by(codigo, periodo) %>%
  summarise(
    n_validos = sum(valido),
    nc = sum(nc_bin[valido], na.rm = TRUE),
    .groups = "drop"
  ) %>%
  tidyr::pivot_wider(
    names_from = periodo,
    values_from = c(n_validos, nc),
    values_fill = 0
  ) %>%
  filter(n_validos_ref >= 20, n_validos_y2024 >= 20) %>%
  mutate(
    p_value = pmap_dbl(
      list(nc_ref, nc_y2024, n_validos_ref, n_validos_y2024),
      ~ prop.test(x = c(..1, ..2), n = c(..3, ..4), correct = FALSE)$p.value
    ),
    p_adj_bh = p.adjust(p_value, method = "BH"),
    nc_ref_pct = 100 * nc_ref / pmax(n_validos_ref, 1),
    nc_2024_pct = 100 * nc_y2024 / pmax(n_validos_y2024, 1),
    delta_pp = nc_2024_pct - nc_ref_pct
  ) %>%
  arrange(desc(delta_pp))

print(head(teste_ponto, 30))

cat("\n===== Diagnostico rapido =====\n")
cat("1) Verifique se o aumento vem de poucos codigos (impacto_pp alto).\n")
cat("2) Verifique mudancas de limite/enquadramento em 2024.\n")
cat("3) Verifique se picos/outliers e repeticoes anormais cresceram em 2024.\n")
cat("4) Use p_adj_bh < 0.05 para focar pontos com mudanca estatisticamente robusta.\n")

