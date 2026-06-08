### Concentration-discharge plots ###

library(tidyverse)
library(here)

nitrate <- read_csv(here("brms model and analysis", "data", "processed", "nitrate_largest_pre_post_covariates.csv"),
                    na = c(".", "-999", "NA"))

## example sites:
# Cherry Ck: 06713500
# Pecos: 08406500
#Green: 09315000
# Virgin: 09415000

ex.dat <- nitrate %>% filter(usgs_site %in% c("USGS-06713500", "USGS-08406500", "USGS-09315000", "USGS-09415000"))
# Data Preparation

fire_colors <- c("before" = "#b5d4f4", "after" = "darkred")

CQex.pl <- ex.dat %>% ggplot(aes(x = log(flow), y = log(value_std), color = segment, fill = segment)) +
                          geom_smooth(method = "lm", se = TRUE) +
                          geom_point(aes(x = log(flow), y = log(value_std), fill = segment), color = "black", pch = 21, size = 2) +
                          scale_color_manual(values = fire_colors, labels = c("after" = "post-fire", "before" = "pre-fire")) +                    
                          scale_fill_manual(values = fire_colors, labels = c("after" = "post-fire", "before" = "pre-fire")) +
                          facet_wrap(~usgs_site, scales = "free") +
                          labs(y = "log(nitrate-N [mg/L])",
                               x = "log(discharge [cfs])") +
                          theme_bw() +
                          theme(legend.position = c(0.875, 0.075),
                                panel.grid.major = element_blank(),
                                panel.grid.minor = element_blank(),
                                panel.background = element_blank(),
                                panel.border = element_rect(colour = "black", fill = NA, linewidth = 2),
                                legend.title = element_blank(),
                                legend.text = element_text(size = 20),
                                strip.background = element_rect(color = "white", fill = "white"),
                                strip.text = element_text(size = 20),
                                axis.text = element_text(size = 20),
                                axis.title = element_text(size = 20))
                          
ggsave(CQex.pl, path = here("CQ"), file = "CQex.pdf", width = 8, height = 8, units = "in")
