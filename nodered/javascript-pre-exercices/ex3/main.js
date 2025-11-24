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

display_prime_number_from_2_to_n(5)