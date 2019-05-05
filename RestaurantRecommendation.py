from flask import Flask, render_template
from flask import request
import csv
         

app = Flask(__name__)

@app.route("/searchZipCode")
def searchZipCode():
    return render_template("searchZipCode.html")

@app.route("/newReview")
def newReview():
    return render_template("newReview.html")

@app.route('/')
def main():
      return render_template("main.html")
	
@app.route("/login")
def login():
    return render_template("login.html")
	
@app.route('/save_new_review', methods=['POST'])
def save_new_review():
    if request.method == 'POST':
        userID = request.form['userID']
        restaurantID = request.form['restaurantID']
        rating = request.form['rating']

        fieldnames = ['userID', 'restaurantID', 'rating']
		
        with open('newReview.csv','a') as inFile:
            writer = csv.DictWriter(inFile, fieldnames=fieldnames)
            writer.writerow({'userID': userID, 'restaurantID': restaurantID, 'rating': rating})

        print("Input recorded!")
        return render_template("newReview.html")

@app.route('/save_search_query', methods=['POST'])
def save_search_query():
    if request.method == 'POST':
        userID = request.form['userID']
        zipcode = request.form['zipcode']

        fieldnames = ['userID', 'zipcode']

        with open('searchList.csv','a') as inFile:
            writer = csv.DictWriter(inFile, fieldnames=fieldnames)
            writer.writerow({'userID': userID, 'zipcode': zipcode})

        print("Input recorded!")
        return render_template("searchZipCode.html")
	
if __name__ == "__main__":
    app.run(debug=True)
#We made two new changes