#include<bits/stdc++.h>
//types
#define llu long long unsigned int
#define ll long long int
//container
#define vi vector<int>
#define ii pair<int,int>
#define vii vector<ii>
//IO
#define pr(n)  printf("%d",n)
#define prl(n) printf("%lld",n)
#define prf(n) printf("%f",n)
#define nl printf("\n")
#define sp printf(" ")
#define sc(n)  scanf("%d",&n)
#define scl(n) scanf("%lld",&n)
#define scf(n) scanf("%f",&n)
#define scd(n) scanf("%f",&n)
//function and loops
#define mp make_pair
#define repab(a,b) for(int i=(a);i<(b);i++)
#define rep(n) for(int i=0;i<n;i++)
#define repi(i,n) for(int (i)=0;(i)<n;(i)++)
#define repit(c) for(auto it=(c).begin();it!=(c).end();it++)
//reading all once
#define read(n) int n;sc(n)
#define readl(n) ll n;scl(n)
#define readf(n) float n;scf(n)
#define readd(n) double d;scd(n)
using namespace std;
template <typename T>
void show_2d(T container){
    for(auto it=container.begin();it!=container.end();it++){
        for(auto it2=it->begin();it2!=it->end();it2++)
            cout<<*it2<<" ";
        cout<<endl;
    }
    cout<<endl;
}
template <typename T>
void show_pair1d(T container){
    for(auto it=container.begin();it!=container.end();it++){
        cout<<it->first<<" --> "<<it->second;
        cout<<endl;
    }
    cout<<endl;
}
template <typename T,typename R>
void show_pair(pair<T,R> p){
    cout<<p.first<<"-> "<<p.second<<endl;
}
template <typename T>
void show_1d(T &container){
    cout<<"data in container is: "<<endl;
    for(auto it=container.begin();it!=container.end();it++){
        cout<<*it<<" ";
    }
    cout<<endl;
}
template <typename  T>
void show_arr(T arr[],int size){
    cout<<"content in array is here"<<endl;
    for (int i = 0; i < size; ++i)
        cout<<arr[i]<<" ";
    cout<<endl;
}

void solve(){
}
vi visited;

vector<vi> graph;
vector<ii>edges;

bool dfs(int a, int b,int n){
    visited[a]=1;
    if(a==b) {
    //    cout<<"True for "<<a<< ", "<<b<<endl;
        return true;
    }

    for(int i=0;i<graph[a].size();i++){
        if(!visited [graph[a][i]]) {
            if(dfs(graph[a][i],b,n)) {
                return true;
            }
        }
    }
    return false;
}
int main()
{
  if(
      freopen("C:\\Users\\ashish_patel\\Documents\\CodeBlocks Project\\Sample\\in.txt","r",stdin)
      &&
      freopen("C:\\Users\\ashish_patel\\Documents\\CodeBlocks Project\\Sample\\out.txt", "w", stdout)
      );
     else {
     cout << "ERROR: " << strerror(errno) << endl;
     exit(0);
    }

//code
     read(n);read(m);
     graph.resize(n+1);
    int a,b;
     edges.push_back(mp(-1,-1));
     rep(m){
         sc(a);sc(b);
         edges.push_back(mp(a,b));
         graph[a].push_back(b);
         graph[b].push_back(a);
     }
    int q;
    sc(q);
    int x;
    int query=0;
    rep(q){
        query++;
        sc(x);
        ii pair=edges[x];
        int a=pair.first;
        int b=pair.second;
       graph[a].erase(find(graph[a].begin(),graph[a].end(),b));
        visited.assign(n+1,0);
        bool ans=dfs(a,b,n);
       if(ans){

           cout<<"Dj Rocks\n";
       }else

           cout<<"Gagan Rocks\n";
        graph[a].push_back(b);
        }

}