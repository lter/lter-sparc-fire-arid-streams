//
// This Stan program defines a linear model of the .
// log concentration (C) - log discharge (Q) relationship.
//
// Learn more about model development with Stan at:
//
//    http://mc-stan.org/users/interfaces/rstan.html
//    https://github.com/stan-dev/rstan/wiki/RStan-Getting-Started
//

// The input data is a vectors 'C' and 'Q' of length 'N'.
data {
  
  int<lower=0> N; // number of observations
  vector[N] C; // observations - chemistry analyte concentration
  vector[N] Q; // predictor - raw daily mean discharge (cfs)
  
}

// The parameters accepted by the model. Our model
// accepts three parameters 'a', 'b', and 'sigma'.
parameters {
  
  real a; // intercept
  real b; // slope
  real<lower=0> sigma; // observation error - must be positive
  
}

// Nothing in the transformed parameters block for now.

transformed parameters{
  
}

// The model to be estimated. We model the output
// 'C' to be normally distributed with the mean value
// using the linear formula and standard deviation 'sigma'.
model {
  
  // Likelihood
  C ~ normal(a + b*Q, sigma);
  
  // model priors - keeping fairly uninformative for now
  a ~ normal(0, 1E-2); // intercept parameter prior
  b ~ normal(0, 1E-1); // slope parameter prior
  // no obs. error prior for now
  // remember, script MUST end in a blank line
  
}

