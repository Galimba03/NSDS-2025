let birth_date = new Date(prompt("Insert your birth date (YYYY-MM-DD): "));
let actual_date = new Date();

let day_years = (actual_date.getFullYear() - birth_date.getFullYear()) * 365;
let day_months = (actual_date.getMonth() - birth_date.getMonth()) * 30;
let day_day = (actual_date.getDay() - birth_date.getDay());

alert(day_years+day_months+day_day);