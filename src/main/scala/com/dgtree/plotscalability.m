x = [ 2, 4, 6, 8];
y = [26 35 50;  16 24 35; 10 24 31; 9.5 16 25];

figure;
b = bar(x, y, 0.75);
b(1).FaceColor = [158/255 204/255 184/255];
b(1).EdgeColor = [72/255 159/255 120/255];

b(2).FaceColor = [255/255 160/255 155/255];
b(2).EdgeColor = [255/255 119/255 111/255];


b(3).FaceColor = [255/255 194/255 141/255];
b(3).EdgeColor = [255/255 156/255 69/255];

xlabel('Number of m4.large instances');
ylabel('Tree Construction time (in minutes)');

legend('76807 Graphs', '2 Lakh Graphs', 'Half Million Graphs');