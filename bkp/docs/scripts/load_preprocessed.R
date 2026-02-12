data_dir <- file.path("data", "preprocessed")

params <- c("iqa", "od", "dbo", "fosfototal")

for (param in params) {
  rds_path <- file.path(data_dir, paste0(param, ".rds"))
  if (!file.exists(rds_path)) {
    stop("Arquivo nao encontrado: ", rds_path)
  }

  data_obj <- readRDS(rds_path)
  assign(paste0("tbl_", param), data_obj$tbl, envir = .GlobalEnv)
  assign(paste0("grouped_obs_", param), data_obj$grouped_obs, envir = .GlobalEnv)
}
