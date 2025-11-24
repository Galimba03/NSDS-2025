let input = parseInt(prompt("Inserisci un numero (>= 2): "));

let prime = true;

for(let i = 2; i<=input; i++) {
    prime = true;
    for(let j = 2; j < i; j++) {
        if(i % j == 0) {
            prime = false;
        }
    }

    if(prime == true) {
        alert(i + " is prime.")
    }
}