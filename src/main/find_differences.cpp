#include <iostream>
#include <string>
#include <bits/stdc++.h>
#include <iostream>
#include <fstream>


using namespace std;

int main()
{   string a01[13565];
    string a10[13625];
    string numbers;
    int counter = 0;

    ifstream file01;
    file01.open("normal.txt");

    ifstream file10;
    file10.open("reversed.txt");

    string str;
    int cnt = 0;
    while (getline(file01, str))
    {
        // Process str
        a01[cnt] = str;
        cnt++;
    }

    cnt = 0;
    while (getline(file10, str))
    {
        // Process str
        a10[cnt] = str;
        cnt++;
    }


    for(int i = 0; i < 13625; ++i){
        //cout << a01[i] << endl;

        // for(int j = 0; j < 13565; ++j){
        //     if(a10[i] == a01[j]){
        //         counter++;
        //         break;
        //     }
        // }
        for(int j = 0; j < 13625; ++j){
            if(a10[i] == a10[j] && i!=j){
                counter++;
                break;
            }
        }
        if(counter != 0){
            cout << a10[i] << endl;
        }
        counter=0;
    }
    // for(int i = 0; i < 13565; ++i){
    //     //cout << a01[i] << endl;

    //     for(int j = 0; j < 13625; ++j){
    //         if(a01[i] == a10[j]){
    //             counter++;
    //             break;
    //         }
    //     }
    //     if(counter == 0){
    //         cout << a01[i] << endl;
    //     }
    //     counter=0;
    // }
    return 0;
}
