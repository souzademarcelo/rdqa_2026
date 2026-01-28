data_path <- file.path("data", "preprocessed", "enquadramento.rds")
if (!file.exists(data_path)) {
  stop("Arquivo nao encontrado: ", data_path, ". Rode scripts/preprocess_enquadramento.R")
}

enq_data <- readRDS(data_path)
assign("enq_estadual_final", enq_data$enq_estadual_final, envir = .GlobalEnv)
assign("enq_uniao_final", enq_data$enq_uniao_final, envir = .GlobalEnv)
