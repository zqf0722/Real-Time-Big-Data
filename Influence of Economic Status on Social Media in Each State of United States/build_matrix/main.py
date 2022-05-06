from geopy.geocoders import Nominatim

if __name__ == '__main__':
    filename = "test.txt"
    outfile = "state_matrix.txt"
    left_most = -125
    right_most = -66
    top_most = 49
    down_most = 26
    m = top_most - down_most + 1
    n = right_most - left_most + 1
    geolocator = Nominatim(user_agent="USA_graph_matrix")
    out_lines = []
    for i in range(m):
        out_line = ""
        for j in range(n):
            print(i, j)
            i_index = i + down_most
            j_index = j + left_most
            location = geolocator.reverse(str(i_index) + "," + str(j_index))
            if not location:
                if j == 0:
                    out_line += "X"
                else:
                    out_line += "," + "X"
            else:
                address = location.raw['address']
                country = address.get('country_code', '')
                state = address.get('state', '')
                if country == "us" and state:
                    if j == 0:
                        out_line += state
                    else:
                        out_line += "," + state
                else:
                    if j == 0:
                        out_line += "X"
                    else:
                        out_line += "," + "X"
            print(out_line)
        out_line += "\n"
        out_lines.append(out_line)
    with open(outfile, 'w') as f:
        f.writelines(out_lines)
    '''
    with open(filename, 'r') as f:
        lines = f.readlines()
        for line in lines:
            print(i)
            i += 1
            line = line[0:-1]
            line = line.split()
            time = line[1][0:10]
            latitude = line[2]
            longitude = line[3]
            id = line[4]
            print(latitude+","+longitude)
            location = geolocator.reverse(latitude + "," + longitude)
            address = location.raw['address']
            country = address.get('country_code', '')
            state = address.get('state', '')
            out_line = country + "\t" + time + "\n"
            out_lines.append(out_line)
    with open(outfile, 'w') as f:
        f.writelines(out_lines)
    '''
