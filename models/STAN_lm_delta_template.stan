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
  vector[N] C; // observations - log-scaled chemistry analyte concentration
  vector[N] Q; // predictor - log-scaled daily mean discharge (cfs)
  int f [N]; // 0/1s denoting pre- and post-fire data, respectively
  
}

// Note, STAN variables may only contain lower and upper case
// Roman letters, numerical digits, and the underscore.

// The parameters accepted by the model. Our model
// accepts three parameters 'a', 'b', and 'sigma'.
parameters {
  
  real A_post; // post-fire log-scaled intercept
  real A_pre; // pre-fire log-scaled intercept
  real b_post; // post-fire slope
  real b_pre; // pre-fire slope
  real<lower=0> sigma_post; // observation error - must be positive
  real<lower=0> sigma_pre; // observation error - must be positive
  real delta; // change in slope
  real sigma_delta; // error of change in slope
  
}

// Nothing in the transformed parameters block for now.

transformed parameters{
  
}

// The model to be estimated. We model the output
// 'C' to be normally distributed with the mean value
// using the linear formula and standard deviation 'sigma'.
model {
  
  // For every available observation, i, run through the
  // pre- or post-fire delineation steps
  for (i in 1:N) {
  
    if (f[i]==1) { // 1 = POST FIRE
  
    // Likelihood
    // log(c) ~ log(a) + b * log(q)
    // or
    // C ~ A + bQ
    C[i] ~ normal(A_post + b_post*Q[i], sigma_post);
  
    }
  
    else { // 0 = PRE FIRE
  
    C[i] ~ normal(A_pre + b_pre*Q[i], sigma_pre);
  
    }
  
  }
  
  // Estimated change in CQ slope between pre- and post-fire periods
  delta ~ normal(b_post - b_pre, sigma_delta);
  
  // Parameter priors - keeping fairly uninformative for now
  A_post ~ normal(0, 1E-2); // post-fire intercept parameter prior
  A_pre ~ normal(0, 1E-2); // pre-fire intercept parameter prior
  b_post ~ normal(0, 1E-1); // post-fire slope parameter prior
  b_pre ~ normal(0, 1E-1); // pre-fire slope parameter prior
  delta ~ normal(0, 1E-1); // change in slope parameter prior
  
  // no obs. or delta error priors for now
  
  // remember, script MUST end in a blank line
  
}

