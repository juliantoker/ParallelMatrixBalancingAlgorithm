import pp,numpy,math

### Server Initialization ###
ppservers = ("*",)
job_server = pp.Server(ppservers=ppservers)

### Constants Declaration ###
A = [[3,0,0], [0,3,0], [0,0,3]]
B = [[1,2],[3,4]]
C = ((0,1,0), (1,0,0), (0,0,1))
D = [[2.0,0],[0,3.0]]
### Basic Function Declarations ###

get_indicies = lambda Matrix: xrange(len(Matrix))
    
def column(Matrix,index):

    """IN: Matrix,int. OUT: Array containing
    the index'th column of the passed in
    matrix."""

    return [row[index] for row in Matrix]

def dot_product(A,B):

    """IN: Two vectors. OUT: The dot
    product of the two passed in vectors."""

    return numpy.dot(A,B)

def task_eval(task):

    """IN: __Task object. OUT:
    literal value of task."""

    return task()

def invert(matrix_element):

    """IN: Real valued matrix element.
    OUT: The reciprocal of the passed in
    argument."""

    return 1/float(matrix_element)

def two_norm(Vector):

    """IN: 1D-Array. OUT: The 2-norm
    of the passed in array."""

    SUM = 0
    
    for entry in Vector:

        SUM += math.pow(entry,2)

    else:

        TWO_NORM = math.sqrt(SUM)
        return TWO_NORM

def norm_compare(row_norm,column_norm):

    """IN: real row/column norms. OUT:
    boolean 1 if row > column, 0 if row < column."""
    
    ROW_LESS_THAN_COLUMN = 0
    ROW_GREATER_THAN_COLUMN = 1
    if row_norm > column_norm:
        
        return ROW_GREATER_THAN_COLUMN

    else: 
        
        return ROW_LESS_THAN_COLUMN

def diagonal_matrix_update(diagonal_matrix_element,update):

    """IN: Diagonal matrix element, and boolean update
    parameter. OUT: Updated diagonal matrix element."""

    INCREMENT_PARAMETER = 0.05
    ROW_LESS_THAN_COLUMN = 0
    ROW_GREATER_THAN_COLUMN = 1
    if update == ROW_LESS_THAN_COLUMN:

        diagonal_matrix_element += INCREMENT_PARAMETER
        return diagonal_matrix_element

    if update == ROW_GREATER_THAN_COLUMN:

        diagonal_matrix_element -= INCREMENT_PARAMETER
        return diagonal_matrix_element

def norm_good_enough(row_norm,column_norm):

    """IN: real row and column 2-norms of some
    unspecified matrix. OUT: Boolean False or True if
    the row and column norms are close enough in
    value to one another."""

    NORM_TOLERANCE = 0.5
    DIFFERENCE = abs(row_norm - column_norm)
    if DIFFERENCE < NORM_TOLERANCE:

        return True

    else:

        return False
    
def nested_job_eval(result_matrix):

    """IN: Matrix. OUT: Matrix with
    literal values for __Task objects."""
    Output = []
    for row in result_matrix:

        new_row = list(map(task_eval,row))
        Output.append(new_row)

    else:

        return Output

def create_zero_matrix(dimension):

    """IN: Int. OUT: Zero matrix of the
    passed in dimension"""

    return [0] * dimension

### Parallel Function Declarations ###
    
def parallel_matrix_multiply(Matrix1,Matrix2):

    """IN: Two Matrices. OUT: The matrix product of
    Matrix1 * Matrix2."""

    Output = []
    Matrix2_Column = Matrix2[0]
    
    for row in Matrix1:

        New_Row = []
        This_Row = row

        for column_index in get_indicies(Matrix2_Column):

            This_Column = column(Matrix2,column_index)
            New_Row.append(job_server.submit(dot_product,(This_Row,This_Column),(),("numpy",)))         

        else:

            Output.append(New_Row)

    else:

        return nested_job_eval(Output)

def parallel_diagonal_matrix_inversion(diagonal_matrix):

    """IN: A diagonal matrix. OUT: The inverse
    of the passed in diagonal matrix."""
    
    jobs = []
    output = []
    for i in get_indicies(diagonal_matrix):

        DIAG_ELEMENT = float(diagonal_matrix[i][i])
        jobs.append(job_server.submit(invert,(DIAG_ELEMENT,)))
        
    else:

        result = [job() for job in jobs]
        for index in get_indicies(result):
            this_row = create_zero_matrix(len(diagonal_matrix))
            this_row[index] = result[index]
            output.append(this_row)

        else:

            return output

def parallel_column_two_norm(Matrix):

    """In: Matrix. OUT: An array containing
    the two-norm of each column in the
    passed in matrix."""

    jobs = []
    for column_index in get_indicies(Matrix[0]):

        Column = column(Matrix,column_index)
        jobs.append(job_server.submit(two_norm,(Column,),(),("math",)))

    else:
        
        return [job() for job in jobs]
        

def parallel_row_two_norm(Matrix):

    """IN: Matrix. OUT: An array containing
    the two norm of each row in the passed
    in matrix."""

    jobs = []
    for row in Matrix:

        jobs.append(job_server.submit(two_norm,(row,),(),("math",)))

    else:

        return [job() for job in jobs]
    
def parallel_norm_compare(row_norms,column_norms):

    """IN: Arrays containing the row and column
    norms of some unspecified matrix. OUT: An array
    containing the vertict of norm_compare() for
    each pair of row and column norms."""

    jobs = []
    for index in get_indicies(row_norms):

        row_norm = row_norms[index]
        column_norm = column_norms[index]
        jobs.append(job_server.submit(norm_compare,(row_norm,column_norm)))

    else:
        
        return [job() for job in jobs]

def parallel_diagonal_matrix_update(matrix,diagonal_matrix):

    """IN: Input matrix, diagonal matrix. OUT: Diagonal matrix with its
    elements updated to balance DAD^(-1)."""
    
    jobs = []
    row_norms = parallel_row_two_norm(matrix)
    column_norms = parallel_column_two_norm(matrix)
    Update_Array = parallel_norm_compare(row_norms,column_norms)
    for index in get_indicies(diagonal_matrix):

        diagonal_element = diagonal_matrix[index][index]
        update = Update_Array[index]
        jobs.append(job_server.submit(diagonal_matrix_update,(diagonal_element,update)))

    else:

        result = [job() for job in jobs]
        for index in get_indicies(result):

            diagonal_matrix[index][index] = result[index]

        else:

            return diagonal_matrix

def parallel_good_enough(matrix):

    """IN: Matrix. OUT: Boolean True or
    False. Returns true if every row/column
    2-norm pair passes the norm_good_enough()
    check."""

    jobs = []
    row_norms = parallel_row_two_norm(matrix)
    column_norms = parallel_column_two_norm(matrix)
    for index in get_indicies(row_norms):

        row_norm = row_norms[index]
        column_norm = column_norms[index]
        jobs.append(job_server.submit(norm_good_enough,(row_norm,column_norm)))

    else:

        result = [job() for job in jobs]
        for boolean in result:

            if boolean == False:

                return False

        else:

            return True

def twod_eigen_compute(matrix):

    first = float((matrix[0][0] + matrix[1][1])/2.0)
    second = 4*matrix[0][1]*matrix[1][0]
    mid = (matrix[0][0] - matrix[1][1])
    third = math.pow(mid,2)
    fourth = second + third
    fifth = (math.sqrt(fourth))/2
    return first + fifth,first - fifth

### The Balancing Algorithm ###
        
def parallel_matrix_balance(matrix,diagonal_matrix):

    """IN: Matrix to be balanced and an arbitrarily
    chosen diagonal matrix. OUT: A balanced version
    of the passed in matrix parameter."""

    holder_matrix = matrix
    
    while parallel_good_enough(holder_matrix) == False:
       
        diagonal_inverse = parallel_diagonal_matrix_inversion(diagonal_matrix)
        intermediate_matrix = parallel_matrix_multiply(diagonal_matrix,matrix)
        holder_matrix = parallel_matrix_multiply(intermediate_matrix,diagonal_inverse)
        diagonal_matrix = parallel_diagonal_matrix_update(holder_matrix,diagonal_matrix)
        
        
    else:

        job_server.print_stats()
        return twod_eigen_compute(holder_matrix),holder_matrix
        



