function isAPrimeNumber(n) {
    for(let i = 2; i<n; i++) {
        if(n%i == 0) {
            return false;
        }
    }
    return true;
}

function display_prime_number_from_2_to_n(n = 5) {
    for(let i = 2; i<=n; i++) {
        if(isAPrimeNumber(i)) {
            alert(i + " is prime.")
        }
    }

}

let primeObjects = [];
function PrimeNumbers (n) {
  this.n = n;
  this.primes = [];
}

let input = parseInt(prompt("Insert a number n: "));

let primeNumber;
for(let i = 2; i <= input; i++) {
    primeNumber = new PrimeNumbers(i);
    for(let j = 2; j <= i; j++) {
        if(isAPrimeNumber(j)) {
            primeNumber.primes.push(j);
        }
    }
    primeObjects.push(primeNumber);
}

console.log(primeObjects);