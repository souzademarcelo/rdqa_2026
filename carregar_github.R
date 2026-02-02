# --- sobrescrever GitHub com o conteúdo local (com validações) ---
repo_dir   <- "C:/pgquali/site-qualidade-agua-final"
remote_name <- "origin"
branch     <- "main"
commit_msg <- "atualização 02 fev 2026"
expected_remote <- "https://github.com/souzademarcelo/rdqa_2026.git"
log_file <- file.path(repo_dir, "push_log.txt")

run_cmd <- function(cmd) {
  cat(sprintf("\n$ %s\n", cmd), file = log_file, append = TRUE)
  out <- system(cmd, intern = TRUE)
  cat(paste(out, collapse = "\n"), file = log_file, append = TRUE)
  cat("\n", file = log_file, append = TRUE)
  return(out)
}

setwd(repo_dir)

# 1) Validar remoto
remotes <- run_cmd("git remote -v")

# pega linha do origin (fetch)
origin_line <- remotes[grepl(paste0("^", remote_name, "\\s+"), remotes) & grepl("\\(fetch\\)$", remotes)][1]

if (length(origin_line) == 0) {
  stop(sprintf("Remoto '%s' não encontrado.", remote_name))
}

# extrai URL completa entre o nome e o (fetch)
origin_url <- sub(paste0("^", remote_name, "\\s+(.+)\\s+\\(fetch\\)$"), "\\1", origin_line)

if (origin_url != expected_remote) {
  stop(sprintf("Remoto não confere. Esperado: %s | Encontrado: %s", expected_remote, origin_url))
}

# 2) Abortar se não houver mudanças
status <- run_cmd("git status --porcelain")
if (length(status) == 0) {
  stop("Sem alterações para commit. Abortado.")
}

# 3) Add, commit, push --force
run_cmd("git add -A")
run_cmd(sprintf('git commit -m "%s"', commit_msg))
run_cmd(sprintf("git push --force %s %s", remote_name, branch))

message("Concluído. Log em: ", log_file)
