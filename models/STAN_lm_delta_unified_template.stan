//
// This Stan program defines a linear model of the .
// log concentration (C) - log discharge (Q) relationship.
//

// The input data is vectors 'C' and 'Q' of length 'N'.
// The data of 'F' and 'f' are intended to delineate pre/post-fire.

data {
  
  int<lower=1> N; // number of observations
  vector[N] C; // observations - log-scaled chemistry analyte concentration
  vector[N] Q; // predictor - log-scaled daily mean discharge (cfs)
  int<lower=1> F; // fire categories (pre/post)
  int<lower=1, upper=F> f [N]; // 1/2s denoting pre- and post-fire data, respectively
  
}

// Note, STAN variables may only contain lower and upper case
// Roman letters, numerical digits, and the underscore.

// Also note, when indexing, numbers must start with 1 not 0.

// The parameters accepted by the model. Our model
// estimates the intercept 'A', the slope 'b', the
// error 'sigma', and finally the change parameter 'delta'. 

// This formulation estimates all parameters as part of one 
// model so as to better pool information within a site.

parameters {
  
  //real mu[D]; // mean
  vector[F] sigma; // observation error - must be positive
  vector[F] b; // CQ slope
  vector[F] A; // log-scaled intercept
  
  
}

// The model to be estimated. We model the output
// 'C' to be normally distributed with the mean value
// using the linear formula and standard deviation 'sigma'.

model {
  
  // The concentration-discharge relationship is modeled as
  // log(c) = log(a) + b * log(q)
  // or
  // C = A + bQ
  for (j in 1:N)
    
    C[j] ~ normal(A[f[j]] + b[f[j]]*Q[j], sigma[f[j]]);
    
    // So, for each indexed parameter, it will find the observation in question,
    // and for that number observation, find the value of the f vector (which
    // denotes a 1 for pre-fire or a 2 for post-fire), and assign that observation
    // to inform that category's parameter estimate, e.g. either b[1] or b[2].
  
  // Parameter priors - keeping fairly uninformative for now
  sigma ~ exponential(0.1); // obs. error must be positive, equivalent to half-normal(0,10)
  A ~ normal(0, 10); // intercept parameter prior
  b ~ normal(0, 10); // slope parameter prior
  
}

// This is where any derived values should be placed, i.e.,
// parameters transformed for reporting purposes. If they 
// are not called by the model, it's more efficient for them
// to be placed here than in a 'transformed parameters' block.

generated quantities{
  
  real delta_b; // change in CQ slope
  real delta_sigma; // change in variation
  
  // Use estimated pre- and post-fire values
  // to calculate the change
  delta_b = b[1] - b[2];
  delta_sigma = sigma[1] - sigma[2];
  
  // remember, script MUST end in a blank line
  
}

