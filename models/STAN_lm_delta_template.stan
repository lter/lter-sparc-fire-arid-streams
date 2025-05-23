//
// This Stan program defines a linear model of the .
// log concentration (C) - log discharge (Q) relationship.
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
// estimates the intercepts 'A_post' and 'A_pre',
// the slopes 'b_post' and 'b_pre', the observation
// errors 'sigma_post' and 'sigma_pre', and finally 
// the change parameters 'delta' and error 'sigma_delta'.
parameters {
  
  real A_post; // post-fire log-scaled intercept
  real A_pre; // pre-fire log-scaled intercept
  real b_post; // post-fire slope
  real b_pre; // pre-fire slope
  real<lower=0> sigma_post; // observation error - must be positive
  real<lower=0> sigma_pre; // observation error - must be positive
  
}

// The models to be estimated. We model the output
// 'C' to be normally distributed with the mean value
// using the linear formula and standard deviation 'sigma'.
// We also model 'delta' to be normally distributed with the mean value
// using the linear formula and standard deviation 'sigma_delta'.
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
  
  // Parameter priors - keeping fairly uninformative for now
  A_post ~ normal(0, 1E-2); // post-fire intercept parameter prior
  A_pre ~ normal(0, 1E-2); // pre-fire intercept parameter prior
  b_post ~ normal(0, 1); // post-fire slope parameter prior
  b_pre ~ normal(0, 1); // pre-fire slope parameter prior
  sigma_post ~ normal(0,1)T[0,]; // obs. error must be positive
  sigma_pre ~ normal(0,1)T[0,]; // same
  
}

// This is where any derived values should be placed, i.e.,
// parameters transformed for reporting purposes. If they 
// are not called by the model, it's more efficient for them
// to be placed here than in a 'transformed parameters' block.

generated quantities{
  
  real delta; // change in slope
  
  // Use estimated pre- and post-fire CQ slopes
  // to calculate the change in slopes
  delta = b_post - b_pre;
  
  // remember, script MUST end in a blank line
  
}

