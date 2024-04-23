//
// This Stan program defines a multi-level linear model of the
// log concentration (C) - log discharge (Q) relationship.
//

// The input data are matrices 'C' and 'Q' of length 'N' by width 'sites'.
// The data of 'F' and 'f' are intended to delineate pre/post-fire.

data {
  
  int<lower=1> sites; // number of sites
  int<lower=1> N; // maximum number of observations at a site 
  // to set bounds on matrices
  int Ndays [sites]; // number of days for each site
  
  matrix [N, sites] C; // observations - log-scaled analyte concentration
  matrix [N, sites] Q; // predictor - log-scaled daily mean discharge (cfs)
  int f[N, sites]; // 1/2s denoting pre- and post-fire data - MUST be an array
  // in order to preserve values inside as integers for calling as indices below
  
  int<lower=1> F; // fire categories (pre/post)
  
}

// Note, STAN variables may only contain lower and upper case
// Roman letters, numerical digits, and the underscore.

// Also note, when indexing, numbers must start with 1 not 0.

// The parameters accepted by the model. Our model
// estimates the intercept 'A', the slope 'b', the
// error 'sigma', and finally the change parameter 'delta'. 

// This formulation estimates all parameters at all sites
// as part of one, hierarachical model so as to better pool
// information across sites.

parameters {
  
  // Site-level parameters
  vector[F] bsigma; // universal CQ slope standard deviation
  matrix [F, sites] bsite; // CQ slope for each site both pre and post-fire
  
  vector[F] Asigma; // universal CQ intercept standard deviation
  matrix [F, sites] Asite; // CQ intercept for each site both pre and post-fire
  
  //vector[F] sigmasigma; // universal CQ variation
  //matrix [F, sites] sigmasite; // CQ variation for each site both pre and post-fire
  
  // Universal & error parameters
  // The first effort with the hierarchical structure will try to get
  // it working using only site-level, and universal parameter estimates.
  // I'll add more groupings later on.
  vector[F] b; // universal CQ slope pre and post-fire
  vector[F] A; // universal log-scaled intercept pre and post-fire
  vector[F] sigma; // universal error pre and post-fire
  
}

// The model to be estimated. We model the output
// 'C' to be normally distributed with the mean value
// using the linear formula and standard deviation 'sigma'.

model {
  
  // The concentration-discharge relationship is modeled as
  // log(c) = log(a) + b * log(q)
  // or
  // C = A + bQ
  
  // Establish loop framework
  // For each of the 17 sites...
  for(h in 1:sites) { 
    
  // and for each day within each of the sites...
  for (j in 1:Ndays[h]) { 
    
    // We first trying the most simple hierarchical approach where
    // parameter_site ~ N(parameter, parameter_sigma)

    C[j,h] ~ normal(Asite[f[j,h],h] + bsite[f[j,h],h]*Q[j,h], sigma[f[j,h]]); 
    // sigma is estimating pre- & post-fire variance
    
    // data structure - compress j,h to i
    
    // So, for each indexed parameter, it will find the observation in question,
    // and for that number observation, find the value of the f vector (which
    // denotes a 1 for pre-fire or a 2 for post-fire), and assign that observation
    // to inform that category's parameter estimate, e.g. either b[1] or b[2].
    
  } // closes daily data loop
  
  // and for each pre-/post-fire index...
  for (k in 1:F){
  
  // Site-level priors
  // Remember, each site-level estimate will actually be two - one pre and one post-fire
  bsite[k,h] ~ normal(b[k], bsigma[k]); // try (0, 10)
  Asite[k,h] ~ normal(A[k], Asigma[k]); // try (0, 10)
  //sigmasite[k,h] ~ exponential(sigma[k]);
  
  // Cauchy distributions are normal dist. with heavy tails
  // bsigma[k] ~ cauchy(0,1);
  // Asigma[k] ~ cauchy(0,1);
  //sigmasigma[k] ~ cauchy(0,1);
  
  // Error priors - keeping values that worked in non-hierarchical format
  sigma[k] ~ exponential(0.1); // error must be positive, equivalent to half-normal(0,10)
  
  // Parameter priors - keeping fairly uninformative for now
  // A[k] ~ normal(0, 10); // intercept parameter prior
  // b[k] ~ normal(0, 10); // slope parameter prior
  
  } // closes pre-/post-fire data loop
  
  } // closes site-level data loop
  
} // closes the MODEL block

// This is where any derived values should be placed, i.e.,
// parameters transformed for reporting purposes. If they 
// are not called by the model, it's more efficient for them
// to be placed here than in a 'transformed parameters' block.

generated quantities{
  
  vector[sites] delta_bsite; // changes in CQ slope by site
  
  // real delta_b; // universal change in CQ slope
  //real delta_sigma; // change in variation
  
  // Use estimated pre- and post-fire values
  // to calculate the change
  for(h in 1:sites) { 
    
  delta_bsite[h] = bsite[1,h] - bsite[2,h];
  
  }
  
  // delta_b = b[1] - b[2];
  //delta_sigma = sigma[1] - sigma[2];
  
  // remember, script MUST end in a blank line !!!
  
}

// hierarchical model
// asite model
// bsite model

// multi-variate regression
// bsite/asite (matrix) ~ basin slope etc. for all covariates (variance/covariance matrix)

