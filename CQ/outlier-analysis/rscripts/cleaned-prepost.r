

library(tidyverse)
library(pwr)

df <- read.csv("CQ/outlier-analysis/data/test_data.csv")
nitrate_df <- read.csv("CQ/outlier-analysis/data/nitrate_working_old.csv")

###########look at difference in number of outliers
##wgt is the percent of obs that are outliers
paired_tests <- function(data, label) { 

  wide <- data %>%
    select(usgs_site, fire_period, wgt) %>%
    pivot_wider(names_from = fire_period, values_from = wgt) %>%
    drop_na()

  pre  <- wide$Prefire
  post <- wide$Postfire
  n    <- nrow(wide)
  d    <- mean(post - pre) / sd(post - pre)

  wilcox <- wilcox.test(pre, post, paired = TRUE, exact = FALSE)
  ttest  <- t.test(pre, post, paired = TRUE)
  list(label = label, n = n, d = d,
       wilcox_p = wilcox$p.value, t_p = ttest$p.value)
}

###power summary that can be run off the paired function
power_summary <- function(result) {
  d <- result$d
  n <- result$n

##maybe need pwr.tn2.test for unequal sample sizes
  current <- pwr.t.test(d = d, n = n, sig.level = 0.05, type = "paired")$power
  
  n_80    <- ceiling(pwr.t.test(d = d, power = 0.80, sig.level = 0.05, type = "paired")$n)
  n_90    <- ceiling(pwr.t.test(d = d, power = 0.90, sig.level = 0.05, type = "paired")$n)

##prints results
  cat("\n  Power analysis (paired t, alpha = 0.05, two-tailed):\n")
  cat("  Current power   :", round(current * 100, 1), "%\n")
  cat("  n for 80% power :", n_80, "\n")
  cat("  n for 90% power :", n_90, "\n")

  invisible(list(current_power = current, n_80 = n_80, n_90 = n_90))
}

##run of each slopedir
slope_result <- c()
for (slope in c("negative", "positive", "neutral")) {
  result <- paired_tests(df %>% filter(slopedir == slope), slope)
  power_summary(result)
  slope_result <- rbind(slope_result, result)
}
slope_result
##across all sites
result_all <- paired_tests(df, "all sites")
power_summary(result_all)

###power curves, values from slope_result
slope_params <- list(
  list(label = "Negative (d = 0.424)", d = 0.424, col = "#3266ad", lty = 1),
  list(label = "Positive (d = 0.309)", d = 0.309, col = "#1d9e75", lty = 2),
  list(label = "Neutral (d = 0.328)",  d = 0.328, col = "#ba7517", lty = 4),
  list(label = "All sites (d = 0.344)", d = 0.344, col = "#7f77dd", lty = 3)
)

n_seq <- 5:150

###this is old code that plots in base
png("../figures/power_curves_number_base.png", width = 800, height = 500, res = 120)
par(mar = c(5, 5, 3, 2))

plot(NULL, xlim = c(5, 150), ylim = c(0, 1),
     xlab = "Sample size (paired sites)", ylab = "Statistical power",
     main = "Power curves — paired t-test (alpha = 0.05)",
     las = 1, bty = "l")

abline(h = 0.80, col = "#e24b4a", lty = 5, lwd = 1.2)
text(148, 0.81, "80%", col = "#e24b4a", adj = 1, cex = 0.8)
abline(h = 0.90, col = "#e24b4a", lty = 5, lwd = 0.8)
text(148, 0.91, "90%", col = "#e24b4a", adj = 1, cex = 0.8)

for (sp in slope_params) {
  powers <- sapply(n_seq, function(n)
    pwr.t.test(d = sp$d, n = n, sig.level = 0.05, type = "paired")$power)
  lines(n_seq, powers, col = sp$col, lty = sp$lty, lwd = 2)
}
current_ns <- list(
  list(d = 0.424, n = 16, col = "#3266ad"),
  list(d = 0.309, n = 19, col = "#1d9e75"),
  list(d = 0.328, n = 21, col = "#ba7517"),
  list(d = 0.344, n = 56, col = "#7f77dd")
)
for (pt in current_ns) {
  pwr <- pwr.t.test(d = pt$d, n = pt$n, sig.level = 0.05, type = "paired")$power
  points(pt$n, pwr, pch = 19, col = pt$col, cex = 1.4)
}
legend("bottomright",
  legend = sapply(slope_params, `[[`, "label"),
  col    = sapply(slope_params, `[[`, "col"),
  lty    = sapply(slope_params, `[[`, "lty"),
  lwd    = 2, bty = "n", cex = 0.85)

dev.off()

summary_stats <- df %>%
  group_by(slopedir, fire_period) %>%
  summarise(
    n_obs      = n(),
    n_sites    = n_distinct(usgs_site),
    mean_pct_obs = round(mean(wgt),   3),
    sd_pct_obs   = round(sd(wgt),     3),
    med_pct_obs  = round(median(wgt), 3),
    q25_pct_obs  = round(quantile(wgt, 0.25), 3),
    q75_pct_obs  = round(quantile(wgt, 0.75), 3),
    .groups    = "drop"
  ) %>%
  arrange(slopedir, fire_period)
 
# also add an "All sites" row
summary_all <- df %>%
  group_by(fire_period) %>%
  summarise(
    slopedir   = "all",
    n_obs      = n(),
    n_sites    = n_distinct(usgs_site),
    mean_pct_obs = round(mean(wgt),   3),
    sd_pct_obs   = round(sd(wgt),     3),
    med_pct_obs  = round(median(wgt), 3),
    q25_pct_obs  = round(quantile(wgt, 0.25), 3),
    q75_pct_obs = round(quantile(wgt, 0.75), 3),
    .groups    = "drop"
  )
 
summary_full <- bind_rows(summary_stats, summary_all) %>%
  arrange(slopedir, fire_period)
 
print(summary_full, n = Inf)
 
# save as CSV
write.csv(summary_full, "../output-csvs/outlier_summary_stats_count.csv", row.names = FALSE)

##nitrate residuals 

##add slopedir
slopedir_lookup <- df %>%
  select(usgs_site, slopedir) %>%
  distinct()

nitrate_df <- nitrate_df %>%
  left_join(slopedir_lookup, by = "usgs_site")

str(nitrate_df)
#j <- sts[1]
sts <- unique(nitrate_df$usgs_site)

####build c-q on prefire then compute residuals of all observations
comb_flagged <- c()
for (j in sts){
    one <- nitrate_df[nitrate_df$usgs_site == j,]
    ##build the model on prefire 
    onepre <- one[one$fire_period == "Prefire",]
    modl <- lm(log(value_std) ~ log(Flow), data=onepre)
    pred <- predict(modl, newdata=one, interval = "confidence")
    one$lwr   = exp(pred[, "lwr"])
    one$upr    = exp(pred[, "upr"])
    one$fit = pred[, "fit"]
    #keep in log
    one$rs <- log(one$value_std) - one$fit
    one$status <- ifelse(one$value_std < one$lwr | one$value_std > one$upr, "Outside CI", "Inside CI")
    one$slopedir <- ifelse(modl$coefficients[2] >= .2, "positive", ifelse(modl$coefficients[2] <= -.2 , "negative", "neutral"))
    comb_flagged <- rbind(comb_flagged, one)
}
str(comb_flagged)

##can just skip to here instead:
write.csv(comb_flagged, "../data/nitrate_working_testing.csv")

##get outliers
outliers <- comb_flagged %>%
  filter(status == "Outside CI")

print(outliers %>% count(fire_period))

##pre-post for all obs
wide <- outliers %>%
    group_by(usgs_site, fire_period) %>%
    summarise(median_resid = median(rs), .groups = "drop") %>%
    pivot_wider(names_from = fire_period, values_from = median_resid) %>%
    drop_na(Prefire, Postfire)

#str(wide)

  pre  <- wide$Prefire
  post <- wide$Postfire
  n    <- nrow(wide) ##number of sites
  d    <- mean(post - pre) / sd(post - pre) ##cohen's d

  wtest <- wilcox.test(pre, post, paired = TRUE, exact = FALSE)
 # data:  pre and post
#V = 679, p-value = 0.2256
  ttest <- t.test(pre, post, paired = TRUE)
# data:  pre and post
# t = 0.76798, df = 46, p-value = 0.4464

# unpaired Mann-Whitney on all outlier values 
pre_vals  <- outliers %>% filter(fire_period == "Prefire")  %>% pull(rs)
post_vals <- outliers %>% filter(fire_period == "Postfire") %>% pull(rs)

utest <- wilcox.test(pre_vals, post_vals, paired = FALSE, exact = FALSE)

pooled_sd <- sqrt((sd(pre_vals)^2 + sd(post_vals)^2) / 2)
d_unpaired <- (mean(post_vals) - mean(pre_vals)) / pooled_sd

# Power for unpaired (one-sample equiv — use pwr.t2n.test for unequal n)
pwr_unpaired <- pwr::pwr.t2n.test(
  n1 = length(pre_vals), n2 = length(post_vals),
  d = abs(d_unpaired), sig.level = 0.05
)

# by slope direction (paired where possible) 

for (slope in c("negative", "positive", "neutral")) {
  sub  <- outliers %>% filter(slopedir == slope)
  pre  <- sub %>% filter(fire_period == "Prefire")  %>% pull(rs)
  post <- sub %>% filter(fire_period == "Postfire") %>% pull(rs)

  if (length(pre) < 3 | length(post) < 3) {
    cat("\nSlopedir:", slope, "— insufficient data (pre n =", length(pre),
        ", post n =", length(post), ")\n")
    next
  }

  utest <- wilcox.test(pre, post, paired = FALSE, exact = FALSE)
  pooled_sd <- sqrt((sd(pre)^2 + sd(post)^2) / 2)
  d_s <- (mean(post) - mean(pre)) / pooled_sd
}
##outlier magnitude

# png("../figures/outlier_magnitude.png", width = 900, height = 500, res = 120)
# par(mfrow = c(1, 2), mar = c(5, 5, 3, 2))

# # Panel 1: all sites combined 
# boxplot(rs ~ fire_period, data = outliers,
#         col  = c("#b5d4f4", "#9fe1cb"),
#         main = "All sites combined",
#         ylab = "Nitrate (value_std)", xlab = "",
#         outline = TRUE, las = 1)
# mtext("Outlier magnitude: Pre vs Post fire", side = 3, line = -1.5, outer = TRUE, font = 2)

# # Panel 2: by slope direction
# outliers$group <- paste(outliers$slopedir, outliers$fire_period, sep = "\n")
# slope_order <- c(
#   "negative\nPrefire", "negative\nPostfire",
#   "positive\nPrefire", "positive\nPostfire",
#   "neutral\nPrefire",  "neutral\nPostfire"
# )
# outliers$group <- factor(outliers$group, levels = slope_order)

# cols <- rep(c("#b5d4f4", "#9fe1cb"), 3)
# boxplot(rs ~ group, data = outliers,
#         col  = cols,
#         main = "By slope direction",
#         ylab = "Nitrate (rs)", xlab = "",
#         outline = TRUE, las = 2, cex.axis = 0.75)

# legend("topright", legend = c("Prefire", "Postfire"),
#        fill = c("#b5d4f4", "#9fe1cb"), bty = "n", cex = 0.85)

# dev.off()


##paired stats - idk why the other function isn't working
run_paired_resid <- function(subset, label) {
  wide <- subset %>%
    group_by(usgs_site, fire_period) %>%
    summarise(median_resid = median(rs), .groups = "drop") %>%
    pivot_wider(names_from = fire_period, values_from = median_resid) %>%
    drop_na(Prefire, Postfire)
 
  n <- nrow(wide)
  if (n < 3) {
    cat("\n", label, ": only", n, "paired sites — skipping\n")
    return(invisible(NULL))
  }
 
  pre  <- wide$Prefire
  post <- wide$Postfire
  d    <- mean(post - pre) / sd(post - pre)
 
  wtest <- wilcox.test(pre, post, paired = TRUE, exact = FALSE)
  ttest <- t.test(pre, post, paired = TRUE)
 
  current <- pwr.t.test(d = abs(d), n = n, sig.level = 0.05, type = "paired")$power
  n_80    <- ceiling(pwr.t.test(d = abs(d), power = 0.80, sig.level = 0.05, type = "paired")$n)
  n_90    <- ceiling(pwr.t.test(d = abs(d), power = 0.90, sig.level = 0.05, type = "paired")$n)
 
  list(label = label, n = n, d = d, power = current, n_80 = n_80, n_90 = n_90,
       wilcox_p = wtest$p.value, t_p = ttest$p.value,
       mean_diff = mean(post - pre), sd_diff = sd(post - pre),
       pre_median = median(pre), post_median = median(post),
       pre_mean = mean(pre), post_mean = mean(post))
}
 
resid_results <- list()
resid_results[["all"]]   <- run_paired_resid(outliers, "All sites")
resid_results[["negative"]] <- run_paired_resid(outliers %>% filter(slopedir == "negative"), "negative")
resid_results[["positive"]] <- run_paired_resid(outliers %>% filter(slopedir == "positive"), "positive")
resid_results[["neutral"]]  <- run_paired_resid(outliers %>% filter(slopedir == "neutral"),  "neutral")
 
#summary stats table 
# Per fire_period x slopedir: n obs, n sites, mean/median/sd of residuals
 
summary_stats <- outliers %>%
  group_by(slopedir, fire_period) %>%
  summarise(
    n_obs      = n(),
    n_sites    = n_distinct(usgs_site),
    mean_resid = round(mean(rs),   3),
    sd_resid   = round(sd(rs),     3),
    med_resid  = round(median(rs), 3),
    q25_resid  = round(quantile(rs, 0.25), 3),
    q75_resid  = round(quantile(rs, 0.75), 3),
    .groups    = "drop"
  ) %>%
  arrange(slopedir, fire_period)
 
# also add an "All sites" row
summary_all <- outliers %>%
  group_by(fire_period) %>%
  summarise(
    slopedir   = "all",
    n_obs      = n(),
    n_sites    = n_distinct(usgs_site),
    mean_resid = round(mean(rs),   3),
    sd_resid   = round(sd(rs),     3),
    med_resid  = round(median(rs), 3),
    q25_resid  = round(quantile(rs, 0.25), 3),
    q75_resid  = round(quantile(rs, 0.75), 3),
    .groups    = "drop"
  )
 
summary_full <- bind_rows(summary_stats, summary_all) %>%
  arrange(slopedir, fire_period)
 
print(summary_full, n = Inf)
 
# save as CSV
write.csv(summary_full, "../output-csvs/outlier_residual_summary_stats.csv", row.names = FALSE)

#power table 
power_rows <- Filter(Negate(is.null), resid_results)
power_table <- do.call(rbind, lapply(power_rows, function(r) {
  data.frame(
    group        = r$label,
    n_paired     = r$n,
    cohens_d     = round(r$d, 3),
    wilcox_p     = round(r$wilcox_p, 4),
    t_p          = round(r$t_p, 4),
    power_pct    = round(r$power * 100, 1),
    n_for_80pct  = r$n_80,
    n_for_90pct  = r$n_90
  )
}))
 
print(power_table, row.names = FALSE)
write.csv(power_table, "../output-csvs/outlier_power_table_count.csv", row.names = FALSE) 

###unpaired Mann-Whitney for reference 
pre_r  <- outliers %>% filter(fire_period == "Prefire")  %>% pull(rs)
post_r <- outliers %>% filter(fire_period == "Postfire") %>% pull(rs)
 
utest     <- wilcox.test(pre_r, post_r, paired = FALSE, exact = FALSE)
pooled_sd <- sqrt((sd(pre_r)^2 + sd(post_r)^2) / 2)
d_u       <- (mean(post_r) - mean(pre_r)) / pooled_sd

group_cols <- c(
  "All sites" = "#7f77dd",
  "negative"  = "#3266ad",
  "positive"  = "#1d9e75",
  "neutral"   = "#ba7517"
)
group_ltys <- c("All sites" = 3, "negative" = 1, "positive" = 2, "neutral" = 4)
 
slope_params_resid <- lapply(Filter(Negate(is.null), resid_results), function(r) {
  list(
    label = sprintf("%s (d = %.3f)", r$label, r$d),
    d     = abs(r$d),
    col   = group_cols[r$label],
    lty   = group_ltys[r$label]
  )
})
 
current_pts <- lapply(Filter(Negate(is.null), resid_results), function(r) {
  list(d = abs(r$d), n = r$n, col = group_cols[r$label])
})
 
n_seq <- 5:300
 
png("../figures/outlier_power_curves.png", width = 800, height = 500, res = 120)
par(mar = c(5, 5, 3, 2))
 
plot(NULL, xlim = c(5, 300), ylim = c(0, 1),
     xlab = "Sample size (paired sites)",
     ylab = "Statistical power",
     main = "Power curves — C-Q residual magnitude (paired t, alpha = 0.05)",
     las = 1, bty = "l")
 
abline(h = 0.80, col = "#e24b4a", lty = 5, lwd = 1.2)
text(298, 0.81, "80%", col = "#e24b4a", adj = 1, cex = 0.8)
abline(h = 0.90, col = "#e24b4a", lty = 5, lwd = 0.8)
text(298, 0.91, "90%", col = "#e24b4a", adj = 1, cex = 0.8)
 
for (sp in slope_params_resid) {
  powers <- sapply(n_seq, function(n)
    pwr.t.test(d = sp$d, n = n, sig.level = 0.05, type = "paired")$power)
  lines(n_seq, powers, col = sp$col, lty = sp$lty, lwd = 2)
}
 
for (pt in current_pts) {
  pwr_pt <- pwr.t.test(d = pt$d, n = pt$n, sig.level = 0.05, type = "paired")$power
  points(pt$n, pwr_pt, pch = 19, col = pt$col, cex = 1.4)
}
 
legend("bottomright",
  legend = sapply(slope_params_resid, `[[`, "label"),
  col    = sapply(slope_params_resid, `[[`, "col"),
  lty    = sapply(slope_params_resid, `[[`, "lty"),
  lwd    = 2, bty = "n", cex = 0.85)
 
dev.off()

#########all plots#######
fire_colours <- c("Prefire" = "#b5d4f4", "Postfire" = "#9fe1cb")
 
slope_labels <- c(
  "negative" = "Negative",
  "positive" = "Positive",
  "neutral"  = "Neutral"
)
 
theme_fire <- theme_bw(base_size = 11) +
  theme(
    panel.grid.minor  = element_blank(),
    strip.background  = element_rect(fill = "grey92", colour = "grey70"),
    legend.position   = "bottom",
    legend.title      = element_blank()
  )
 
# plot1 
outliers_plot <- outliers %>%
  mutate(
    fire_period = factor(fire_period, levels = c("Prefire", "Postfire")),
    slopedir    = factor(slopedir, levels = c("negative", "positive", "neutral"),
                         labels = slope_labels)
  )
####only plot the median residual per site to avoid overplotting
str(outliers_plot)
outliers_plot2 <- outliers_plot %>%
  group_by(usgs_site,fire_period) %>%
  summarise(median_rs = median(rs), slopedir = first(slopedir), .groups = "drop")

p_resid_all <- ggplot(outliers_plot2, aes(x = fire_period, y = median_rs, fill = fire_period)) +
  geom_boxplot(outlier.size = 0.8, outlier.alpha = 0.4, width = 0.5) +
  geom_hline(yintercept = 0, linetype = "dashed", colour = "grey40") +
  scale_fill_manual(values = fire_colours) +
  labs(
    title    = "All sites combined",
    x        = NULL,
    y        = "Outlier Magnitue"
  ) +
  theme_fire +
  theme(legend.position = "none")

p_resid_all

p_resid_slope <- ggplot(outliers_plot2, aes(x = fire_period, y = median_rs, fill = fire_period)) +
  geom_boxplot(outlier.size = 0.8, outlier.alpha = 0.4, width = 0.5) +
  geom_hline(yintercept = 0, linetype = "dashed", colour = "grey40") +
  scale_fill_manual(values = fire_colours) +
  facet_wrap(~ slopedir, nrow = 1) +
  labs(
    title = "By slope direction",
    x     = NULL,
    y     = "Outlier Magnitue"
  ) +
  theme_fire + 
  theme(legend.position = "none")

p_resid_slope 

p_resid_combined <- cowplot::plot_grid(
  p_resid_all, p_resid_slope,
  ncol        = 2,
  rel_widths  = c(1, 2.2),
  labels      = c("A", "B"),
  label_size  = 12
)

p_resid_combined 

ggsave("../figures/outlier_residuals_gg.png", p_resid_combined, width = 10, height = 4.5, dpi = 150)
 
#plot2

colnames(comb_flagged)
comb_flagged$inx <- 1
outlier_counts <- comb_flagged %>%
  mutate(
    fire_period = factor(fire_period, levels = c("Prefire", "Postfire")),
    slopedir    = factor(slopedir, levels = c("negative", "positive", "neutral"),
                         labels = slope_labels)
  ) %>%
  group_by(usgs_site, fire_period, slopedir) %>%
  summarise(
    n_outliers = sum(inx[status == "Outside CI"]),
    n_total    = n(),
    pct_out    = n_outliers / n_total,
    .groups    = "drop"
  )
 
p_count_all <- ggplot(outlier_counts,
                      aes(x = fire_period, y = pct_out, fill = fire_period)) +
  geom_boxplot(outlier.size = 0.8, outlier.alpha = 0.4, width = 0.5) +
  scale_fill_manual(values = fire_colours) +
  labs(
    title = "All sites combined",
    x     = NULL,
    y     = "No. outliers per site"
  ) +
  theme_fire +
  theme(legend.position = "none")
 
p_count_slope <- ggplot(outlier_counts,
                        aes(x = fire_period, y = pct_out, fill = fire_period)) +
  geom_boxplot(outlier.size = 0.8, outlier.alpha = 0.4, width = 0.5) +
  scale_fill_manual(values = fire_colours) +
  facet_wrap(~ slopedir, nrow = 1) +
  labs(
    title = "By slope direction",
    x     = NULL,
    y     = "No. outliers per site"
  ) +
  theme_fire +
  theme(legend.position = "none")
 
p_count_combined <- cowplot::plot_grid(
  p_count_all, p_count_slope,
  ncol       = 2,
  rel_widths = c(1, 2.2),
  labels     = c("A", "B"),
  label_size = 12
)

ggsave("../figures/outlier_counts.png", p_count_combined, width = 10, height = 4.5, dpi = 150)
 
#plot3 power curves 
 
group_colours <- c(
  "All sites" = "#7f77dd",
  "negative"  = "#3266ad",
  "positive"  = "#1d9e75",
  "neutral"   = "#ba7517"
)
group_linetypes <- c(
  "All sites" = "dotted",
  "negative"  = "solid",
  "positive"  = "dashed",
  "neutral"   = "dotdash"
)

# build power curve data from resid_results
power_curve_df <- do.call(rbind, lapply(Filter(Negate(is.null), resid_results), function(r) {
  ns <- 5:300
  data.frame(
    n          = ns,
    power      = sapply(ns, function(n)
                   pwr.t.test(d = abs(r$d), n = n,
                              sig.level = 0.05, type = "paired")$power),
    group      = r$label,
    current_n  = r$n,
    current_pw = pwr.t.test(d = abs(r$d), n = r$n,
                             sig.level = 0.05, type = "paired")$power
  )
}))
 
# current-n marker points (one row per group)
current_pts_df <- power_curve_df %>%
  distinct(group, current_n, current_pw)
 
p_power <- ggplot(power_curve_df, aes(x = n, y = power,
                                       colour = group, linetype = group)) +
  geom_line(linewidth = 0.9) +
  geom_point(data = current_pts_df,
             aes(x = current_n, y = current_pw, colour = group),
             size = 3, shape = 19, show.legend = FALSE) +
  geom_hline(yintercept = 0.80, linetype = "dashed",
             colour = "#e24b4a", linewidth = 0.7) +
  geom_hline(yintercept = 0.90, linetype = "dashed",
             colour = "#e24b4a", linewidth = 0.4, alpha = 0.7) +
  annotate("text", x = 298, y = 0.82, label = "80%",
           colour = "#e24b4a", hjust = 1, size = 3.2) +
  annotate("text", x = 298, y = 0.92, label = "90%",
           colour = "#e24b4a", hjust = 1, size = 3.2) +
  scale_colour_manual(
    values = group_colours,
    labels = function(x) {
      sapply(x, function(g) {
        r <- Filter(Negate(is.null), resid_results)[[g]]
        if (is.null(r)) g else sprintf("%s  (d = %.3f)", r$label, r$d)
      })
    }
  ) +
  scale_linetype_manual(
    values = group_linetypes,
    labels = function(x) {
      sapply(x, function(g) {
        r <- Filter(Negate(is.null), resid_results)[[g]]
        if (is.null(r)) g else sprintf("%s  (d = %.3f)", r$label, r$d)
      })
    }
  ) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1),
                     limits = c(0, 1)) +
  scale_x_continuous(limits = c(5, 300)) +
  labs(
    title   = "Power curves \u2014 C-Q residual magnitude (paired t, \u03b1 = 0.05)",
    x       = "Sample size (paired sites)",
    y       = "Statistical power",
    colour  = NULL,
    linetype = NULL
  ) +
  theme_fire +
  theme(legend.position = "right")
 
ggsave("../figures/outlier_power_curves_magnitude.png", p_power, width = 8, height = 5, dpi = 150)

 # plot4
count_results <- list()
for (grp_name in c("all", "negative", "positive", "neutral")) {
  sub <- if (grp_name == "all") outlier_counts else outlier_counts %>% filter(slopedir == slope_labels[grp_name])
  lbl <- if (grp_name == "all") "All sites" else grp_name
 
  wide_c <- sub %>%
    select(usgs_site, fire_period, pct_out) %>%
    pivot_wider(names_from = fire_period, values_from = pct_out) %>%
    drop_na(Prefire, Postfire)
 
  n <- nrow(wide_c)
  if (n < 3) next
 
  pre_c  <- wide_c$Prefire
  post_c <- wide_c$Postfire
  d_c    <- mean(post_c - pre_c) / sd(post_c - pre_c)
 
  cp  <- pwr.t.test(d = abs(d_c), n = n, sig.level = 0.05, type = "paired")$power
  n80 <- ceiling(pwr.t.test(d = abs(d_c), power = 0.80, sig.level = 0.05, type = "paired")$n)
  n90 <- ceiling(pwr.t.test(d = abs(d_c), power = 0.90, sig.level = 0.05, type = "paired")$n)
 
  count_results[[grp_name]] <- list(
    label = lbl, n = n, d = d_c, power = cp, n_80 = n80, n_90 = n90
  )
}
resid_results
power_rows <- Filter(Negate(is.null), count_results)
power_table <- do.call(rbind, lapply(power_rows, function(r) {
  data.frame(
    group        = r$label,
    n_paired     = r$n,
    cohens_d     = round(r$d, 3),
    wilcox_p     = round(r$wilcox_p, 4),
    t_p          = round(r$t_p, 4),
    power_pct    = round(r$power * 100, 1),
    n_for_80pct  = r$n_80,
    n_for_90pct  = r$n_90
  )
}))
 
print(power_table, row.names = FALSE)
write.csv(power_table, "../output-csvs/outlier_power_table.csv", row.names = FALSE) 
 
count_curve_df <- do.call(rbind, lapply(Filter(Negate(is.null), count_results), function(r) {
  ns <- 5:300
  data.frame(
    n          = ns,
    power      = sapply(ns, function(n)
                   pwr.t.test(d = abs(r$d), n = n,
                              sig.level = 0.05, type = "paired")$power),
    group      = r$label,
    current_n  = r$n,
    current_pw = pwr.t.test(d = abs(r$d), n = r$n,
                             sig.level = 0.05, type = "paired")$power
  )
}))
 
count_pts_df <- count_curve_df %>% distinct(group, current_n, current_pw)

n_seq <- 5:150
p_power_count <- ggplot(count_curve_df, aes(x = n, y = power,
                                             colour = group, linetype = group)) +
  geom_line(linewidth = 0.9) +
  geom_point(data = count_pts_df,
             aes(x = current_n, y = current_pw, colour = group),
             size = 3, shape = 19, show.legend = FALSE) +
  geom_hline(yintercept = 0.80, linetype = "dashed",
             colour = "#e24b4a", linewidth = 0.7) +
  geom_hline(yintercept = 0.90, linetype = "dashed",
             colour = "#e24b4a", linewidth = 0.4, alpha = 0.7) +
  annotate("text", x = 298, y = 0.82, label = "80%",
           colour = "#e24b4a", hjust = 1, size = 3.2) +
  annotate("text", x = 298, y = 0.92, label = "90%",
           colour = "#e24b4a", hjust = 1, size = 3.2) +
  scale_colour_manual(
    values = group_colours,
    labels = function(x) sapply(x, function(g) {
      r <- Filter(Negate(is.null), count_results)[[g]]
      if (is.null(r)) g else sprintf("%s  (d = %.3f)", r$label, r$d)
    })
  ) +
  scale_linetype_manual(
    values = group_linetypes,
    labels = function(x) sapply(x, function(g) {
      r <- Filter(Negate(is.null), count_results)[[g]]
      if (is.null(r)) g else sprintf("%s  (d = %.3f)", r$label, r$d)
    })
  ) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1),
                     limits = c(0, 1)) +
  scale_x_continuous(limits = c(5, 300)) +
  labs(
    title    = "Power curves \u2014 outlier counts per site (paired t, \u03b1 = 0.05)",
    x        = "Sample size (paired sites)",
    y        = "Statistical power",
    colour   = NULL,
    linetype = NULL
  ) +
  theme_fire +
  theme(legend.position = "right")
 
ggsave("../figures/count_power_curves.png", p_power_count, width = 8, height = 5, dpi = 150)
