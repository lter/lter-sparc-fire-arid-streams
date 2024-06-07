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
  
  int<lower=1> regions; // number of HUC regions
  int<lower=0> N; // maximum possible number of observations
  int Nobs [regions]; // number of observations within each region
  
  matrix[N, regions] delta; // observations - mean estimates of change in CQ slopes from posterior probability distributions
  matrix[N, regions] delta_sd; // variability of observations - sd estimates of change in CQ slopes from posterior probability distributions
  matrix[N, regions] B; // predictor - percent of the watershed burned (%)
  
}

// The parameters accepted by the model. Our model
// accepts three parameters 'a', 'b', and 'sigma'.

// This formulation estimates all parameters at all regions
// as part of one, hierarachical model so as to better pool
// information across sites.

parameters {
  
  // HUC level parameters
  real b_Bsigma; // universal slope s.d.
  vector[regions] b_Bregion; // slope for each region
  
  real asigma; // universal intercept s.d.
  vector[regions] aregion; // intercept for each region
  
  // regression parameters
  real a; // universal intercept
  real b_B; // universal slope parameter for % watershed burned
  
  // error parameters
  real<lower=0> sigma; // universal observation error
  // does sigma need to be positive??
  
}

// Nothing in the transformed parameters block for now.

transformed parameters{
  
}

// The model to be estimated. We model the output
// 'delta' to be normally distributed with the mean value
// using the linear formula and standard deviation 'sigma'.
model {
  
  // Establish loop framework
  // For each of the 3 regions...
  for(h in 1:regions){
    
  // and for each observation within each region...
  //for(j in 1:Nobs[h]){
  
  // Likelihood
  delta[h] ~ normal(aregion[h] + b_Bregion[h]*B[h], sigma);
  
  // regional model priors 
  aregion[h] ~ normal(a, asigma); // intercept parameter prior
  b_Bregion[h] ~ normal(b_B, b_Bsigma); // slope parameter prior
  
  // error priors
  sigma ~ normal(mean(delta_sd[h]), sd(delta_sd[h]));
  
  //} // closes observations loop
  
  } // closes regions for loop
  
  // remember, script MUST end in a blank line
  
}


