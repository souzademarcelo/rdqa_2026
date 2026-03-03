rmarkdown::render_site()

# --- sobrescrever GitHub com o conteúdo local (com validações) ---
repo_dir       <- "C:/pgquali/site-qualidade-agua-final"
remote_name    <- "origin"
branch         <- "main"
commit_msg     <- "atualização 03 mar 2026"
expected_remote <- "https://github.com/souzademarcelo/rdqa_2026.git"

git_user_name  <- "Marcelo Souza"
git_user_email <- "souzademarcelo@gmail.com"

log_file <- file.path(repo_dir, "push_log.txt")

run_cmd <- function(cmd) {
  cat(sprintf("\n$ %s\n", cmd), file = log_file, append = TRUE)
  out <- system(cmd, intern = TRUE)
  cat(paste(out, collapse = "\n"), file = log_file, append = TRUE)
  cat("\n", file = log_file, append = TRUE)
  return(out)
}

setwd(repo_dir)

# 0) garantir identidade do git (apenas neste repo)
run_cmd(sprintf('git config user.name "%s"', git_user_name))
run_cmd(sprintf('git config user.email "%s"', git_user_email))

# 1) validar remoto
remotes <- run_cmd("git remote -v")
origin_line <- remotes[grepl(paste0("^", remote_name, "\\s+"), remotes) & grepl("\\(fetch\\)$", remotes)][1]

if (length(origin_line) == 0) {
  stop(sprintf("Remoto '%s' não encontrado.", remote_name))
}

origin_url <- sub(paste0("^", remote_name, "\\s+(.+)\\s+\\(fetch\\)$"), "\\1", origin_line)

if (origin_url != expected_remote) {
  stop(sprintf("Remoto não confere. Esperado: %s | Encontrado: %s", expected_remote, origin_url))
}

# 2) abortar se não houver mudanças
status <- run_cmd("git status --porcelain")
if (length(status) == 0) {
  stop("Sem alterações para commit. Abortado.")
}

# 3) add, commit, push --force
run_cmd("git add -A")
run_cmd(sprintf('git commit -m "%s"', commit_msg))
run_cmd(sprintf("git push --force %s %s", remote_name, branch))

message("Concluído. Log em: ", log_file)
