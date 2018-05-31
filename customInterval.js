function customInterval(callback, interval) {
  let start = new Date();
  let runs = 0;
  (function innerInterval(callback, interval) {
    // Call passed function
    callback();
    // Keep track of current timestamp after callback() is finished
    let currentDone = new Date();

    // Increment run count. We need this to calculate timeOutInterval based on start time.
    runs++;
    // Calculate next Timeout interval
    let nextTimeOut = (runs * interval) - (currentDone - start);
    // Set timeout to call this function again
    setTimeout(function () {
      innerInterval(callback, interval);
    }, nextTimeOut);
  })(callback, interval);
}