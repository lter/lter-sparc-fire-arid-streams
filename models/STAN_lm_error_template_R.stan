//
// This Stan program defines a linear model of the
// change in CQ slope to percent of the watershed burned relationship.
//
// Learn more about model development with Stan at:
//
//    http://mc-stan.org/users/interfaces/rstan.html
//    https://github.com/stan-dev/rstan/wiki/RStan-Getting-Started
//

// The input data are vectors 'delta', 'delta_sd', and 'B' of length 'N'.
data {
  
  int<lower=0> N; // maximum possible number of observations
  vector[N] delta; // observations - mean estimates of change in CQ slopes from posterior probability distributions
  vector[N] delta_sd; // variability of observations - sd estimates of change in CQ slopes from posterior probability distributions
  vector[N] B; // predictor - percent of the watershed burned (%)
  
}

// The parameters accepted by the model. Our model
// accepts three parameters 'a', 'b', and 'sigma'.

parameters {
  
  real a; // intercept
  real b; // slope
  
}

// Nothing in the transformed parameters block for now.

transformed parameters{
  
}

// The model to be estimated. We model the output
// 'delta' to be normally distributed with the mean value
// using the linear formula and standard deviation 'sigma'.
model {
  
  // Likelihood
  delta ~ normal(a + b*B, delta_sd);
  // place delta_sd in place of sigma
  
  // model priors - keeping fairly uninformative for now
  a ~ normal(0, 10); // intercept parameter prior
  b ~ normal(0, 10); // slope parameter prior
  
  // remember, script MUST end in a blank line
  
}


