let temp = -1;
let temp_array = []

while(temp != 0) {
    temp = parseFloat(prompt("Insert a temperature [0 to stop]: "));

    temp_array.push(temp);
    console.log("Added " + temp);
}

let max = temp_array[0];
let min = temp_array[0];
let sum = temp_array[0];

for(let i = 1; i<temp_array.length; i++) {
    max = max < temp_array[i] ? temp_array[i] : max;
    min = min > temp_array[i] ? temp_array[i] : min;

    sum += temp_array[i]
}

console.log(
    "Min: " + min +
    " | Max: " + max +
    " | Avg: " + sum/(temp_array.length-1)
)