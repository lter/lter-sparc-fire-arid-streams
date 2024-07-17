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
  
  // data necessary for region-level grouping
  int<lower=0> N; // number of observations
  int<lower=1> R; // total number of regions
  int<lower=1, upper=R> region [N]; // integers denoting regions
  // note, indexing needs to start with 1
  
  // data necessary to fit regression
  vector[N] delta; // observations - mean estimates of change in CQ slopes from posterior probability distributions
  vector[N] delta_sd; // variability of observations - sd estimates of change in CQ slopes from posterior probability distributions
  vector[N] B; // predictor - percent of the watershed burned (%)
  
}

// The parameters accepted by the model. Our model
// accepts two parameters 'aregion' and 'b_Bregion'.

// This formulation estimates all parameters at all regions.

parameters {
  
  // HUC level parameters
  vector[R] b_Bregion; // slope for each region
  
  vector[R] aregion; // intercept for each region
  
  // no hyperparameters in this version
  
}

// Nothing in the transformed parameters block for now.

transformed parameters{
  
}

// The model to be estimated. We model the output
// 'delta' to be normally distributed with the mean value
// using the linear formula and standard deviation 'delta_sd'.
model {
  
  // Establish loop framework
  // For each observation j...
  for(j in 1:N){
  
  // Likelihood
  delta[j] ~ normal(aregion[region[j]] + b_Bregion[region[j]]*B[j], delta_sd[j]);
  // delta, B, and delta_sd are at site-level
  
  // regional model priors 
  aregion ~ normal(0, 10); // no hyperparameters
  b_Bregion ~ normal(0, 10); // no hyperparameters
  
  } // closes regions for loop
  
  // remember, script MUST end in a blank line
  
}


