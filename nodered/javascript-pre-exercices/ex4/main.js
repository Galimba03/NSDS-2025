function AddressBook(name, address, civicNumber, zipCode) {
    this.name = name;
    this.address = address;
    this.civicNumber = civicNumber;
    this.zipCode = zipCode;
    this.info = function() {
        console.log(
            "Name: " + this.name +
            " | Address: " + this.address +
            " | Civic number: " + this.civicNumber +
            " | Zip code: " + zipCode 
        )
    }
};

let name = prompt("Insert your name:");
let address = prompt("Insert your address: ");
let civicNumber = prompt("Insert your civic number: ");
let zipCode = prompt("Insert your zip code: ");

let my_address_book = new AddressBook(name, address, civicNumber, zipCode);
my_address_book.info();